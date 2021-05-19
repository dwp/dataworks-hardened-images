package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsSyncClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;

public class ClientUtility {

    public static AWSLogs amazonLogsClient(String region) {
        return useDevEnvironmentRemotely() ? assumedRoleLogsClient(region) : logsClient(region);
    }

    private static boolean useDevEnvironmentRemotely() {
        return "true".equals(System.getenv("AWS_USE_DEVELOPMENT_REMOTELY"));
    }

    private static AWSLogs assumedRoleLogsClient(String region) {
        return assumedRoleClientBuilder(AWSLogsClientBuilder.standard(), region);
    }

    private static AWSLogs logsClient(String region) {
        return AWSLogsClientBuilder.standard().withRegion(region).build();
    }

    private static <B extends AwsSyncClientBuilder<B, C>, C> C assumedRoleClientBuilder(B builder, String region) {
        Credentials credentials = credentials(region);
        return builder.withRegion(region).withCredentials(new AWSStaticCredentialsProvider(
                new BasicSessionCredentials(credentials.getAccessKeyId(), credentials.getSecretAccessKey(),
                        credentials.getSessionToken()))).build();
    }

    private static Credentials credentials(String region) {
        AssumeRoleRequest roleRequest = new AssumeRoleRequest().withRoleArn(System.getenv("AWS_ASSUMED_ROLE"))
                                                               .withRoleSessionName("development-session")
                                                               .withDurationSeconds(60 * 60);
        AWSSecurityTokenService client = AWSSecurityTokenServiceClientBuilder.standard().withRegion(region).build();
        AssumeRoleResult result = client.assumeRole(roleRequest);
        return result.getCredentials();
    }

    public static AmazonDynamoDB amazonDynamoDb(String region) {
        return useDevEnvironmentRemotely() ? assumedRoleDynamoDbClient(region) : dynamoDbClient(region);
    }

    private static AmazonDynamoDB assumedRoleDynamoDbClient(String region) {
        return assumedRoleClientBuilder(AmazonDynamoDBClientBuilder.standard(), region);
    }

    private static AmazonDynamoDB dynamoDbClient(String region) {
        return AmazonDynamoDBClientBuilder.standard().withRegion(region).build();
    }

    public static AWSLambda amazonLambda(String region) {
        return useDevEnvironmentRemotely() ? assumedRoleLambdaClient(region) : lambdaClient(region);
    }

    private static AWSLambda assumedRoleLambdaClient(String region) {
        return assumedRoleClientBuilder(AWSLambdaClientBuilder.standard(), region);
    }

    private static AWSLambda lambdaClient(String region) {
        return AWSLambdaClientBuilder.standard().withRegion(region).build();
    }

    public static AmazonElasticMapReduce amazonElasticMapReduce(String region) {
        return useDevEnvironmentRemotely() ? assumedRoleEmrClient(region) : emrClient(region);
    }

    private static AmazonElasticMapReduce assumedRoleEmrClient(String region) {
        return assumedRoleClientBuilder(AmazonElasticMapReduceClientBuilder.standard(), region);
    }

    private static AmazonElasticMapReduce emrClient(String region) {
        return AmazonElasticMapReduceClientBuilder.standard().withRegion(region).build();
    }
}
