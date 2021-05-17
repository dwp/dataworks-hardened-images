package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsSyncClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;

public class AwsUtility {

    public static AmazonDynamoDB amazonDynamoDb(String region) {
        return "true".equals(System.getenv("AWS_USE_DEVELOPMENT_REMOTELY")) ?
                assumedRoleDynamoDbClient(region) :
                dynamoDbClient(region);
    }

    public static AWSLambda amazonLambda(String region) {
        return "true".equals(System.getenv("AWS_USE_DEVELOPMENT_REMOTELY")) ?
                assumedRoleLambdaClient(region) :
                lambdaClient(region);
    }

    private static AmazonDynamoDB dynamoDbClient(String region) {
        return AmazonDynamoDBClientBuilder.standard().withRegion(region).build();
    }

    private static AmazonDynamoDB assumedRoleDynamoDbClient(String region) {
        return assumedRoleClientBuilder(AmazonDynamoDBClientBuilder.standard(), region);
    }

    private static AWSLambda lambdaClient(String region) {
        return AWSLambdaClientBuilder.standard().withRegion(region).build();
    }

    private static AWSLambda assumedRoleLambdaClient(String region) {
        return assumedRoleClientBuilder(AWSLambdaClientBuilder.standard(), region);
    }

    private static <B extends AwsSyncClientBuilder<B, C>, C> C assumedRoleClientBuilder(B builder, String region) {
        Credentials credentials = credentials(region);
        return builder.withRegion(region).withCredentials(new AWSStaticCredentialsProvider(
                new BasicSessionCredentials(credentials.getAccessKeyId(), credentials.getSecretAccessKey(),
                        credentials.getSessionToken()))).build();
    }

    private static Credentials credentials(String region) {
        AssumeRoleRequest roleRequest = new AssumeRoleRequest().withRoleArn(System.getenv("AWS_ASSUMED_ROLE"))
                .withRoleSessionName("development-session");
        AWSSecurityTokenService client = AWSSecurityTokenServiceClientBuilder.standard().withRegion(region).build();
        AssumeRoleResult result = client.assumeRole(roleRequest);
        return result.getCredentials();
    }
}
