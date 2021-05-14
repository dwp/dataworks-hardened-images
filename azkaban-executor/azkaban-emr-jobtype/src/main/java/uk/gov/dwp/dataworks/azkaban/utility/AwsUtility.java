package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

public class AwsUtility {

    public static AmazonDynamoDB amazonDynamoDb(String region) {
        return "true".equals(System.getenv("AWS_USE_LOCALSTACK")) ?
                AmazonDynamoDBClientBuilder.standard()
                        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(LOCALSTACK_END_POINT,region))
                        .withClientConfiguration(new ClientConfiguration().withProtocol(Protocol.HTTP))
                        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(LOCALSTACK_ACCESS_KEY,
                                LOCALSTACK_SECRET_KEY)))
                        .build() :
                AmazonDynamoDBClientBuilder.standard()
                        .withRegion(region)
                        .build();
    }

    private final static String LOCALSTACK_END_POINT = "http://localstack:4566/";
    private final static String LOCALSTACK_ACCESS_KEY = "accessKey";
    private final static String LOCALSTACK_SECRET_KEY = "secretKey";
}
