#! /bin/sh


prog=$(basename $0)


main() {
    local -r profile=${1:?Usage: $prog $(args)}
    local -r cluster=${2:?Usage: $prog $(args)}
    local -r task_definition=${3:?Usage: $prog $(args)}

    if stop_service $profile $cluster; then
        start_service $profile $cluster $task_definition
    else
        echo Failed to shut down web server
        return 1
    fi
}

stop_service() {
    local -r profile=${1:?Usage $FUNCNAME profile cluster}
    local -r cluster=${2:?Usage $FUNCNAME profile cluster}

    aws --profile $profile --no-paginate ecs update-service --cluster $cluster \
        --service azkaban-webserver \
        --force-new-deployment \
        --desired-count 0  > /dev/null

    timeout 10m bash <<EOF
    while [[ \$(running_count $profile $cluster) -gt 0 ]]; do
        echo Waiting for container count to reach 0.
        sleep 10
    done
EOF

}

start_service() {
    local -r profile=${1:?Usage: $prog $(args)}
    local -r cluster=${2:?Usage: $prog $(args)}
    local -r task_definition=${3:?Usage: $prog $(args)}

    aws --profile $profile --no-paginate \
        ecs update-service \
        --cluster $cluster \
        --service azkaban-webserver --task-definition $task_definition \
        --desired-count 1 > /dev/null

    timeout 10m bash <<EOF
    while [[ \$(running_count $profile $cluster) -eq 0 ]]; do
        echo Waiting for container count to exceed 0.
        sleep 10
    done
EOF
}

running_count() {
    local -r profile=${1:?Usage $FUNCNAME profile cluster}
    local -r cluster=${2:?Usage $FUNCNAME profile cluster}
    aws --profile $profile ecs describe-services --cluster $cluster \
        --services azkaban-webserver | jq -r '.services[0].runningCount'
}

export -f running_count

args() {
    echo profile cluster task-definition
}


main $@
