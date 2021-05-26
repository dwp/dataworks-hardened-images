# Azkaban EMR Custom job types

## EMR Launcher job

### Overview

A job to bring-up a pre-configured cluster by invoking its launching-lambda. The job 
additionally monitors the cluster as it starts up and runs its steps while relaying the 
logs from the steps back to the console. 

### High level operation

The job must be provided with the dependencies that need to have completed before the
cluster can be launched. 

The dependencies are either the name of a product that must have completed in its entirety
before the EMR can be launched or the name of a product and a list of collections. When a 
list of collection is supplied the job only has to wait for the subset of data described by the 
collections to be ready before launching the EMR.

The job first polls the pipeline metadata table (in the case of a product being supplied 
as the dependency) or the `UCExportToCrownStatus` table (in the case of a product and a 
list of collections being supplied) until the dependencies have finished or until a timeout 
occurs. Only if the dependencies finish successfully within the timeout period does the job 
invoke the lambda which launches the cluster.

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
| `collection.dependencies` | A comma separated list of collection names that the process should look up on the UCExportToCrownStatus` table to determine when it's safe to start the cluster. |
| `data.product`            | The data product name, e.g. `clive` |
| `emr.launcher.lambda`     | The name of the launching-lambda, this property is mandatory, there is no default. |
| `export.date`             | The date the dependency occured on - used to find the relevant row in the pipeline metadata table, defaults to today's date in the format `YYYY-MM-DD`. |
| `job.dependencies`        | A comma separated list of jobs to check for on the metadata table, must have at least one value. |
| `notification.topic.name` | Which SNS topic to send notifications to, defaults to `Monitoring` |

### Local running.

A small app (`uk.gov.dwp.dataworks.azkaban.app.App`) which instantiates an EMRJobStep instance and calls its run method 
is provided which can be altered at will. To enable this app set the following 2 environment variables in the 
application's run configuration in your IDE:

| Variable name                  | Description                            |
|--------------------------------|----------------------------------------|
| `AWS_USE_DEVELOPMENT_REMOTELY` | Set to `true` to enable local running. |
| `AWS_ASSUMED_ROLE`             | Set to the arn of the role that the app should assume - i.e. the development administrator role, e.g. `arn:aws:iam:000000000000:role/administrator` |

### A note on step logs

In order to display the logs for a step on the azkaban console the plugin must be able to determine which log 
streams in which log group to fetch log events from.

The log group must be supplied in the job config. The plugin will then get a list of all log streams in that group. The 
plugin reduces this list of log streams to those whose names contain an instance id of any instance in the currently 
running cluster (thus removing dead log streams from instances in terminated clusters).

It then further reduces the list of log streams to those that contain the name of the currently running step, this 
should yield a single log stream and it is that log stream whose events are returned.

This strategy may not work for every single job that is to be run by the plugin in which case additional strategies will 
have to be added and used where appropriate. 
