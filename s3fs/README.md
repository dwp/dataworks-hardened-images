# DataWorks S3FS 

[S3FS-FUSE](https://github.com/s3fs-fuse/s3fs-fuse) Docker image that manages mounting user
and shared team directories in the DataWorks Tooling Environments. 

It uses the same S3 bucket for both locations. The user directory is created under `<s3_bucket>/home/<username>`, and the
team directory under `<s3_bucket>/shared/<team_name>`.

Different KMS keys can be specified for user/team locations. Both `KMS_HOME` and `KMS_SHARED` env variables must be provided.

The Docker images requires elevated privileges: the `SYS_ADMIN` kernel capability and access to the `/dev/fuse` device.
Therefore it is recommended not to provide direct access to this Docker container. The recommended way of deploying is as
a sidecar container, the S3FS mounts being accessed through a shared mount through the Docker host.

Processes in other containers must run under a user with UID 1001 in order to read/write data from/to the s3fs mounts.


The docker image requires the following environment variables:
* `S3_BUCKET` - s3 bucket id to mount
* `KMS_HOME` - KMS key ARN to use for the home location, aliases not supported
* `KMS_SHARED` - KMS key ARN to use for the shared location, aliases not supported
* `USER` - Used to created the home sub-directory (`<s3_bucket>/home/${USER}`)
* `TEAM` - Used to created the team sub-directory (`<s3_bucket>/home/${TEAM}`)
