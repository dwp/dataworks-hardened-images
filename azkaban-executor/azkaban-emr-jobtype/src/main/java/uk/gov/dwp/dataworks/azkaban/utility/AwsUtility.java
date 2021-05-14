package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.*;

public class AwsUtility {

    public static AmazonDynamoDB amazonDynamoDb(String region) {
        return "true".equals(System.getenv("AWS_USE_LOCALSTACK")) ?
                    localstackClient(region) :
                "true".equals(System.getenv("AWS_USE_DEVELOPMENT_REMOTELY")) ?
                    assumedRoleClient() :
                    regularClient();
    }

    private static AmazonDynamoDB regularClient() {
        return AmazonDynamoDBClientBuilder.standard().build();
    }

    private static AmazonDynamoDB localstackClient(String region) {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(LOCALSTACK_END_POINT, region))
                .withClientConfiguration(new ClientConfiguration().withProtocol(Protocol.HTTP)).withCredentials(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(LOCALSTACK_ACCESS_KEY, LOCALSTACK_SECRET_KEY))).build();
    }

    private static AmazonDynamoDB assumedRoleClient() {
        AssumeRoleRequest roleRequest = new AssumeRoleRequest()
                .withRoleArn(System.getenv("AWS_ASSUMED_ROLE"))
                .withRoleSessionName("development-session");

        AWSSecurityTokenService client = AWSSecurityTokenServiceClientBuilder.standard().withRegion("eu-west-2").build();
        AssumeRoleResult result = client.assumeRole(roleRequest);
        Credentials credentials = result.getCredentials();
        return AmazonDynamoDBClientBuilder.standard().withRegion("eu-west-2")
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicSessionCredentials(credentials.getAccessKeyId(),
                                credentials.getSecretAccessKey(), credentials.getSessionToken()))).build();
    }

    private final static String LOCALSTACK_END_POINT = "http://localstack:4566/";
    private final static String LOCALSTACK_ACCESS_KEY = "accessKey";
    private final static String LOCALSTACK_SECRET_KEY = "secretKey";
}
