# azkaban-webserver

## A containerised Azkaban webserver for use with EMR

## Description
Azkaban uses webservers to requisition and monitor tasks that are carried out by Azkaban executors. They also serve the frontend (web UI) and retrieve logs from cloudwatch and aggregate them for each job. This implementation has the addition of a purpose built CognitoUserManager plus a proxy and an auth helper to allow for users within a Cognito userpool to be respected within the Azkaban app

## Development
It is key to understand the way that the containerised implementation of Azkaban works, in order to carry out development work and test the changes to the webserver image. The webservers rely upon an RDS table in order to track the currently available executors that they can send tasks to. In order to ensure that only your webserver is up you can follow the below:
1. Build and push your image to ECR - development images can be tagged with `debug`, so as not to affect higher environments:
   ```shell
    docker build -t <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/azkaban-webserver:debug .
    docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/azkaban-webserver:debug
    ```
   Or, you can use the command `make push-webserver-to-ecr` if you add in the missing vars in the Makefile [here](https://github.com/dwp/dataworks-hardened-images/blob/master/azkaban-webserver/Makefile)
1. Create a revised task definition for the service that points at the image tagged `debug`.
1. All active instances of webservers must be torn down. This can be done using the AWS console or AWS CLI:
    ```shell
    aws ecs update-service --cluster <CLUSTER_NAME> --service azkaban-webserver --force-new-deployment --desired-count 0 
    ```
1. Once the Webservers are down, they can be brought back up using the console or CLI:
    ```shell
    aws ecs update-service --cluster <CLUSTER_NAME> --service azkaban-webserver --desired-count <INT> 
    ```
    *The new task definition will be used by default, if it is the latest active one but, it can be passed in to the cli command, if needed using `--task-definition <TASK_DEFINITION_NAME>:<REVISION_NUMBER>`

This should ensure that the executors are registered with the new webservers and all testing will be carried out on the updated image.

You can also run the script in the Executor README.md found [here](https://github.com/dwp/dataworks-hardened-images/tree/master/azkaban-executor/README.md) but, this will unnecessarily destroy all executors. 
