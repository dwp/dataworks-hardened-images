SHELL:=bash

aws_mgmt_dev_account=""
aws_default_region=""
temp_image_tag=""

push-webserver-to-ecr: ## Push a temp version of the consumer to AWS MGMT-DEV ECR
	@{ \
		aws ecr get-login-password --region $(aws_default_region) --profile dataworks-management-dev | docker login --username AWS --password-stdin $(aws_mgmt_dev_account).dkr.ecr.$(aws_default_region).amazonaws.com; \
		docker tag azkaban-webserver $(aws_mgmt_dev_account).dkr.ecr.$(aws_default_region).amazonaws.com/azkaban-webserver:$(temp_image_tag); \
		docker push $(aws_mgmt_dev_account).dkr.ecr.$(aws_default_region).amazonaws.com/azkaban-webserver:$(temp_image_tag); \
	}
