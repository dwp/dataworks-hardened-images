SHELL:=bash

aws_mgmt_dev_account=POPULATE_ME
aws_default_region=POPULATE_ME

build:
	docker build -t azkaban-webserver .

publish:
	@{ \
		aws ecr get-login-password --region $(aws_default_region) --profile dataworks-management-dev | docker login --username AWS --password-stdin $(aws_mgmt_dev_account).dkr.ecr.$(aws_default_region).amazonaws.com; \
		docker tag azkaban-webserver $(aws_mgmt_dev_account).dkr.ecr.$(aws_default_region).amazonaws.com/azkaban-webserver:development; \
		docker push $(aws_mgmt_dev_account).dkr.ecr.$(aws_default_region).amazonaws.com/azkaban-webserver:development; \
	}
