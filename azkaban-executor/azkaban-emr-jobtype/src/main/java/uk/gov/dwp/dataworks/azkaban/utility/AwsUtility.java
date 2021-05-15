package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;

public class AwsUtility {

    public static AmazonDynamoDB amazonDynamoDb(String region) {
        return "true".equals(System.getenv("AWS_USE_DEVELOPMENT_REMOTELY")) ? assumedRoleClient(region) : regularClient(region);
    }

    private static AmazonDynamoDB regularClient(String region) {
        return AmazonDynamoDBClientBuilder.standard().withRegion(region).build();
    }

    private static AmazonDynamoDB assumedRoleClient(String region) {
        AssumeRoleRequest roleRequest = new AssumeRoleRequest().withRoleArn(System.getenv("AWS_ASSUMED_ROLE"))
                .withRoleSessionName("development-session");

        AWSSecurityTokenService client = AWSSecurityTokenServiceClientBuilder.standard().withRegion(region).build();
        AssumeRoleResult result = client.assumeRole(roleRequest);
        Credentials credentials = result.getCredentials();
        return AmazonDynamoDBClientBuilder.standard().withRegion(region).withCredentials(
                new AWSStaticCredentialsProvider(
                        new BasicSessionCredentials(credentials.getAccessKeyId(), credentials.getSecretAccessKey(),
                                credentials.getSessionToken()))).build();
    }

}
