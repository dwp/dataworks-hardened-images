# Azkaban EMR Custom job types

## EMR Launcher job

### Overview

A job to bring-up a pre-configured cluster by invoking its launching-lambda. The job 
additionally monitors the cluster as it starts up and runs its steps while relaying the 
logs from the steps back to the console. 

### High level operation

The job must be provided with the dependencies that need to have completed before the
cluster can be launched. The job first polls the pipeline metadata table until the 
dependencies have finished or until a timeout occurs. Only if the dependencies finish
successfully within the timeout period does the job invoke the lambda which 
launches the cluster.

Assuming the dependencies do complete successfully, the cluster-launching-lambda 
is invoked, the lambda is supplied with the data from the pipeline metadata 
table in the payload (correlation id, s3 prefixes of source data etc.).

If the launcher completes successfully then it returns the cluster id in its result 
payload. The cluster status is then polled repeatedly until it reaches 'RUNNING' status
at this point the pre-configured steps will commence.

If the cluster does reach the 'RUNNING' state then attention switches to the running 
steps. The currently running step is monitored until it finishes (normally or abnormally)
while this is happening the logs from the step are retrieved from AWS and sent to the 
console.

Only if all the steps run to a successful completion is the job deemed to be 
successful and at this point an SNS message is posted which results in a slack message 
announcing the successful run. If the status is not reached the job is deemed a failure
and a message is posted indicating this resulting in a slack message to the same effect.

### Configuration

The azkaban job configuration file must contain certain key value pairs for the job to be
able to perform its task though some have defaults, only those without a default are mandatory.
The table below gives the details.

| Key                       | Description   |
|---------------------------|---------------|
| `aws.log.group.name`      | Log group that the step log streams are a part of. If not supplied no logs will be fetched |
| `cluster.name`            | The name of the cluster, to be used in the notifications, if not supplied the name of the launching lambda is used |
| `data.product`            | The data product name, e.g. `clive` |
| `emr.launcher.lambda`     | The name of the launching-lambda, this property is mandatory, there is no default. |
| `export.date`             | The date the dependency occured on - used to find the relevant row in the pipeline metadata table, defaults to today's date in the format `YYYY-MM-DD`. |
| `job.dependencies`        | A comma separated list of jobs to check for on the metadata table, must have at least one value. |
| `notification.topic.name` | Which SNS topic to send notifications to, defaults to `Monitoring` |
| `pipeline.metadata.table` | Which DynamoDb table to search for dependency status, default is `data_pipeline_metadata` |

### Local running.

A small app (`uk.gov.dwp.dataworks.azkaban.app.App`) which instantiates an EMRJobStep instance and calls its run method 
is provided which can be altered at will. To enable this app set the following 2 environment variables in the 
application's run configuration in your IDE:

| Variable name                  | Description                            |
|--------------------------------|----------------------------------------|
| `AWS_USE_DEVELOPMENT_REMOTELY` | Set to `true` to enable local running. |
| `AWS_ASSUMED_ROLE`             | Set to the arn of the role that the app should assume - i.e. the development administrator role, e.g. `arn:aws:iam:000000000000:role/administrator` |
