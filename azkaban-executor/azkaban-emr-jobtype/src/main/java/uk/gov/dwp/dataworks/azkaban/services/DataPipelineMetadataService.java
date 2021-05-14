package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

public class DataPipelineMetadataService {

    public DataPipelineMetadataService(AmazonDynamoDB dynamoDB) {
        this.dynamoDb = dynamoDB;
    }

    public boolean isCompleted(String product, String exportDate) {
        return false;
    }

    private final AmazonDynamoDB dynamoDb;
}
