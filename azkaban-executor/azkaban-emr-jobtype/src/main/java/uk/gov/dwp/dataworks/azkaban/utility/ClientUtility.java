package uk.gov.dwp.dataworks.azkaban.utility;

import com.amazonaws.ClientConfiguration;
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
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;

public class ClientUtility {

    public static AmazonSNS amazonSNS(String region) {
        return useDevEnvironmentRemotely() ? assumedRoleSnsClient(region) : snsClient(region);
    }

    public static AWSLambda amazonLambda(String region) {
        return useDevEnvironmentRemotely() ? assumedRoleLambdaClient(region) : lambdaClient(region);
    }

    public static AmazonDynamoDB amazonDynamoDb(String region) {
        return useDevEnvironmentRemotely() ? assumedRoleDynamoDbClient(region) : dynamoDbClient(region);
    }

    public static AWSLogs awsLogs(String region) {
        return useDevEnvironmentRemotely() ? assumedRoleLogsClient(region) : logsClient(region);
    }

    public static AmazonElasticMapReduce amazonElasticMapReduce(String region) {
        return useDevEnvironmentRemotely() ? assumedRoleEmrClient(region) : emrClient(region);
    }

    private static AWSLogs assumedRoleLogsClient(String region) {
        return assumedRoleClientBuilder(AWSLogsClientBuilder.standard(), region);
    }

    private static AWSLogs logsClient(String region) {
        return clientBuilder(AWSLogsClientBuilder.standard(), region);
    }

    private static AmazonDynamoDB assumedRoleDynamoDbClient(String region) {
        return assumedRoleClientBuilder(AmazonDynamoDBClientBuilder.standard(), region);
    }

    private static AmazonDynamoDB dynamoDbClient(String region) {
        return clientBuilder(AmazonDynamoDBClientBuilder.standard(), region);
    }

    private static AWSLambda assumedRoleLambdaClient(String region) {
        return assumedRoleClientBuilder(AWSLambdaClientBuilder.standard(), region);
    }

    private static AWSLambda lambdaClient(String region) {
        return clientBuilder(AWSLambdaClientBuilder.standard(), region);
    }

    private static AmazonSNS assumedRoleSnsClient(String region) {
        return assumedRoleClientBuilder(AmazonSNSClientBuilder.standard(), region);
    }

    private static AmazonSNS snsClient(String region) {
        return clientBuilder(AmazonSNSClientBuilder.standard(), region);
    }

    private static AmazonElasticMapReduce assumedRoleEmrClient(String region) {
        return assumedRoleClientBuilder(AmazonElasticMapReduceClientBuilder.standard(), region);
    }

    private static AmazonElasticMapReduce emrClient(String region) {
        return clientBuilder(AmazonElasticMapReduceClientBuilder.standard(), region);
    }

    private static <B extends AwsSyncClientBuilder<B, C>, C> C assumedRoleClientBuilder(B builder, String region) {
        Credentials credentials = credentials(region);
        return builder.withRegion(region).withCredentials(new AWSStaticCredentialsProvider(
                new BasicSessionCredentials(credentials.getAccessKeyId(), credentials.getSecretAccessKey(),
                        credentials.getSessionToken()))).build();
    }

    private static <B extends AwsSyncClientBuilder<B, C>, C> C clientBuilder(B builder, String region) {
        return builder.withRegion(region)
                      .withClientConfiguration(new ClientConfiguration().withSocketTimeout(120_000))
//                      .withRegion(region)
                      .build();
    }

    private static Credentials credentials(String region) {
        AssumeRoleRequest roleRequest = new AssumeRoleRequest().withRoleArn(System.getenv("AWS_ASSUMED_ROLE"))
                                                               .withRoleSessionName("development-session")
                                                               .withDurationSeconds(60 * 60);
        AWSSecurityTokenService client = AWSSecurityTokenServiceClientBuilder.standard().withRegion(region).build();
        AssumeRoleResult result = client.assumeRole(roleRequest);
        return result.getCredentials();
    }

    private static boolean useDevEnvironmentRemotely() {
        return "true".equals(System.getenv("AWS_USE_DEVELOPMENT_REMOTELY"));
    }

}
