#! /bin/sh

set -x

prog=$(basename $0)


main() {
    OPTIND=1

    local options=":s:"

    while getopts $options opt; do
        case $opt in
            s)
                local -r service_override=$OPTARG
                ;;
            \?)
                stderr Usage: $FUNCNAME [ $options ]
                return 2
                ;;
        esac
    done

    shift $((OPTIND - 1))



    local -r profile=${1:?Usage: $prog $(args)}
    local -r cluster=${2:?Usage: $prog $(args)}
    local -r task_definition=${3:?Usage: $prog $(args)}

    local -r service=${service_override:-azkaban-external-webserver}

    if stop_service $profile $cluster $service; then
        start_service $profile $cluster $service $task_definition
    else
        echo Failed to shut down web server
        return 1
    fi
}

stop_service() {
    local -r profile=${1:?Usage $FUNCNAME profile cluster service}
    local -r cluster=${2:?Usage $FUNCNAME profile cluster service}
    local -r service=${3:?Usage $FUNCNAME profile cluster service}

    aws --profile $profile \
        --no-paginate \
        ecs update-service \
        --cluster $cluster \
        --service $service \
        --force-new-deployment \
        --desired-count 0  > /dev/null

    timeout 10m bash <<EOF
    while [[ \$(running_count $profile $cluster $service) -gt 0 ]]; do
        echo Waiting for container count to reach 0.
        sleep 10
    done
EOF

}

start_service() {
    local -r profile=${1:?Usage: $FUNCNAME profile cluster service task-definition}
    local -r cluster=${2:?Usage: $FUNCNAME profile cluster service task-definition}
    local -r service=${3:?Usage $FUNCNAME profile cluster service task-definition}
    local -r task_definition=${4:?Usage: $FUNCNAME profile cluster service task-definition}

    aws --profile $profile --no-paginate \
        ecs update-service \
        --cluster $cluster \
        --service $service \
        --task-definition $task_definition \
        --desired-count 1 > /dev/null

    timeout 10m bash <<EOF
    while [[ \$(running_count $profile $cluster $service) -eq 0 ]]; do
        echo Waiting for container count to exceed 0.
        sleep 10
    done
EOF
}

running_count() {
    local -r profile=${1:?Usage $FUNCNAME profile cluster service}
    local -r cluster=${2:?Usage $FUNCNAME profile cluster service}
    local -r service=${3:?Usage $FUNCNAME profile cluster service}

    aws --profile $profile ecs describe-services --cluster $cluster \
        --services $service | jq -r '.services[0].runningCount'
}

export -f running_count

args() {
    echo [ -s service ] profile cluster task-definition
}


main $@
