# hue

### Description
A Hue web-app image that is built on Alpine and configured to run backed onto an AWS EMR cluster.

### Additional startup configurations
 - Setup mariadb database using JWT token as auth
 - ThriftServer -> EMR
 - Grant permissions 
 - Set up keys 
 - Periodic backup job
 - Run [supervisor](http://supervisord.org/) in the background
 - Add custom `hue-overrides.ini` for general Hue config overrides

### Local Development
To develop the image, make changes to the codebase then build the image and run the image locally as explained below.

#### Building locally:
Run `docker build -t <IMAGE_TAG> ./hue` from the root of the module

This may take some time, as the image requires the building of multiple python wheels.

#### Running Locally: 
Run `docker run -d -p'8080:8888' <IMAGE_TAG>` and navigate to http://localhost:8080/ on your local machine