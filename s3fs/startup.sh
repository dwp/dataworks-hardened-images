#!/bin/sh

set -eu

mkdir -p /mnt/s3fs/s3-home && mkdir -p /mnt/s3fs/s3-shared


# Mount entire S3 bucket with home KMS key and set permission metadata for user home dir

mkdir -p /mnt/tmp

/opt/s3fs-fuse/bin/s3fs ${S3_BUCKET} /mnt/tmp -f \
    -o ecs \
    -o endpoint=eu-west-2 \
    -o url=https://s3.amazonaws.com \
    -o use_sse=kmsid:${KMS_HOME} &>/dev/null &

S3FS_PID=$!

while [ -z "$(ls /mnt/tmp/home)" ]; do
    echo "Waiting for mount";
    sleep 1;
done

chmod -R a-r /mnt/tmp/home/${USER} # || true Needed as this will fail if there are some files encrypted with a different KMS key

fusermount -u /mnt/tmp
kill -9 $S3FS_PID || true

# Mount entire S3 bucket with shared KMS key and set permission metadata for shared dir

/opt/s3fs-fuse/bin/s3fs ${S3_BUCKET} /mnt/tmp -f \
    -o ecs \
    -o endpoint=eu-west-2 \
    -o url=https://s3.amazonaws.com \
    -o use_sse=kmsid:${KMS_SHARED} &>/dev/null &

S3FS_PID=$!

while [ -z "$(ls /mnt/tmp/shared)" ]; do
    echo "Waiting for mount";
    sleep 1;
done

chmod -R a-r /mnt/tmp/shared || true # || true Needed as this will fail if there are some files encrypted with a different KMS key

fusermount -u /mnt/tmp
kill -9 $S3FS_PID || true

rm -rf /mnt/tmp

nohup /opt/s3fs-fuse/bin/s3fs ${S3_BUCKET}:/home/${USER} /mnt/s3fs/s3-home -f \
    -o allow_other \
    -o ecs \
    -o endpoint=eu-west-2 \
    -o url=https://s3.amazonaws.com \
    -o use_sse=kmsid:${KMS_HOME} &> /var/log/s3fs-home &

nohup /opt/s3fs-fuse/bin/s3fs ${S3_BUCKET}:/shared /mnt/s3fs/s3-shared -f \
    -o allow_other \
    -o ecs \
    -o endpoint=eu-west-2 \
    -o url=https://s3.amazonaws.com \
    -o use_sse=kmsid:${KMS_SHARED} &> /var/log/s3fs-shared &

tail -f /var/log/s3fs-home -f /var/log/s3fs-shared
