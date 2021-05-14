terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  access_key                  = "access_key_id"
  region                      = "eu-west-2"
  s3_force_path_style         = true
  secret_key                  = "secret_access_key"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    dynamodb = "http://localstack:4566"
  }
}

resource "aws_dynamodb_table" "data_pipeline_metadata" {
  name         = "data_pipeline_metadata"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "Correlation_Id"
  range_key    = "DataProduct"

  attribute {
    name = "Correlation_Id"
    type = "S"
  }

  attribute {
    name = "DataProduct"
    type = "S"
  }
}

resource "aws_dynamodb_table_item" "adg_incremental_entry" {
  table_name = aws_dynamodb_table.data_pipeline_metadata.name
  hash_key   = aws_dynamodb_table.data_pipeline_metadata.hash_key
  range_key = aws_dynamodb_table.data_pipeline_metadata.range_key

  item = <<EOF
  {
    "Date": {
        "S": "2021-02-12"
    },
    "Status": {
        "S": "Completed"
    },
    "DataProduct": {
        "S": "ADG-incremental"
    },
    "Correlation_Id": {
        "S": "ingest_emr_scheduled_tasks_export_snapshots_to_crown_production_incremental_79_incremental"
    },
    "Run_Id": {
        "N": "1"
    }
  }
EOF

}
