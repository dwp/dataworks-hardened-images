package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class PipelineMetadataService {

    public PipelineMetadataService(AmazonDynamoDB dynamoDB) {
        this.dynamoDb = dynamoDB;
    }

    public Optional<List<Map<String, AttributeValue>>> successfulDependencies(String tableName, String exportDate, String ... products) {
        return dependenciesSucceeded(tableName, dependenciesMetadata(tableName, exportDate, products)) ?
            Optional.of(dependenciesMetadata(tableName, exportDate, products)) : Optional.empty();
    }

    public void cancel() {
        logger.warn("Operation has been cancelled");
        this.latch.countDown();
    }

    private List<Map<String, AttributeValue>> dependenciesMetadata(String tableName, String exportDate, String ... products) {
        return Arrays.stream(products).map(product -> dependenciesMetadata(tableName, product, exportDate))
                .reduce(new ArrayList<>(), (accumulation, next) -> {
                    accumulation.addAll(next);
                    return accumulation;
                });
    }

    private boolean dependenciesSucceeded(String tableName, List<Map<String, AttributeValue>> items) {
        return items.stream().map(item -> completedSuccessfully(tableName, item)).filter(x -> !x).findFirst().orElse(true);
    }

    private boolean completedSuccessfully(String tableName, Map<String, AttributeValue> item) {
        try {
            final AtomicBoolean succeeded = new AtomicBoolean(false);
            final ScheduledExecutorService dependencyCheckExecutor = Executors.newSingleThreadScheduledExecutor();
            this.latch = new CountDownLatch(1);
            dependencyCheckExecutor.scheduleAtFixedRate(() -> checkDependency(tableName, item, succeeded), 0,
                    pollIntervalSeconds(), TimeUnit.MILLISECONDS);
            final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
            timeoutExecutor.schedule(latch::countDown, pollTimeoutSeconds(), TimeUnit.MILLISECONDS);
            latch.await();
            timeoutExecutor.shutdownNow();
            dependencyCheckExecutor.shutdownNow();
            return succeeded.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkDependency(String tableName, Map<String, AttributeValue> item, AtomicBoolean succeeded) {
        logger.info("Checking '" + itemString(item) + "'");
        GetItemRequest request = new GetItemRequest().withTableName(tableName).withKey(primaryKey(item));
        GetItemResult result = dynamoDb.getItem(request);
        String status = result.getItem().get(STATUS_FIELD).getS();
        logger.info("Checked '" + itemString(item) + "', status is '" + status + "'");
        if (hasFinished(status)) {
            logger.info("Dependency '" + itemString(item) + "' has completed, status is '" + status + "'");
            succeeded.set(status.equals(SUCCESSFUL_COMPLETION_STATUS));
            latch.countDown();
        }
    }

    private List<Map<String, AttributeValue>> dependenciesMetadata(String tableName, String product, String exportDate) {
        List<Map<String, AttributeValue>> results = new ArrayList<>();
        Map<String, AttributeValue> lastKeyEvaluatedKey = null;
        do {
            ScanRequest request = scanRequest(tableName, product, exportDate, lastKeyEvaluatedKey);
            ScanResult result = dynamoDb.scan(request);
            results.addAll(result.getItems());
            lastKeyEvaluatedKey = result.getLastEvaluatedKey();
        } while (lastKeyEvaluatedKey != null);

        return results;
    }

    private static Map<String, AttributeValue> primaryKey(Map<String, AttributeValue> item) {
        return item.entrySet().stream()
                .filter(entry -> entry.getKey().equals(PARTITION_KEY_FIELD) || entry.getKey()
                        .equals(SORT_KEY_FIELD)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static ScanRequest scanRequest(String tableName, String product, String exportDate, Map<String, AttributeValue> lastKeyEvaluatedKey) {
        return new ScanRequest().withTableName(tableName)
                .withFilterExpression("#product = :product and #export_date = :export_date")
                .withExclusiveStartKey(lastKeyEvaluatedKey).withExpressionAttributeNames(nameMap())
                .withExpressionAttributeValues(valueMap(product, exportDate));
    }

    private static Map<String, AttributeValue> valueMap(String product, String exportDate) {
        Map<String, AttributeValue> valueMap = new HashMap<>();
        valueMap.put(":export_date", new AttributeValue().withS(exportDate));
        valueMap.put(":product", new AttributeValue().withS(product));
        return valueMap;
    }

    private static Map<String, String> nameMap() {
        Map<String, String> nameMap = new HashMap<>();
        nameMap.put("#export_date", "Date");
        nameMap.put("#product", "DataProduct");
        return nameMap;
    }

    private static boolean hasFinished(String status) {
        System.out.println("HAS FINISHED: '" + status + "': '" + (status.equals(SUCCESSFUL_COMPLETION_STATUS) || status.equals(FAILED_COMPLETION_STATUS)) + "'");
        return status.equals(SUCCESSFUL_COMPLETION_STATUS) || status.equals(FAILED_COMPLETION_STATUS);
    }

    private static String itemString(Map<String, AttributeValue> item) {
        return item.get(PARTITION_KEY_FIELD).getS() + "/" + item.get(SORT_KEY_FIELD).getS() + "/" + item.get(DATE_FIELD);
    }

    private static int pollIntervalSeconds() {
        return Integer.parseInt(System.getProperty("poll.interval.milliseconds", "10000"));
    }

    private static int pollTimeoutSeconds() {
        return Integer.parseInt(System.getProperty("poll.timeout.milliseconds", "3600000"));
    }

    private final static String PARTITION_KEY_FIELD = "Correlation_Id";
    private final static String SORT_KEY_FIELD = "DataProduct";
    private final static String STATUS_FIELD = "Status";
    private final static String DATE_FIELD = "Date";
    private final static String SUCCESSFUL_COMPLETION_STATUS = "Completed";
    private final static String FAILED_COMPLETION_STATUS = "Failed";
    private final AmazonDynamoDB dynamoDb;
    private CountDownLatch latch = new CountDownLatch(1);
    private final static Logger logger = LoggerFactory.getLogger(PipelineMetadataService.class);
}
