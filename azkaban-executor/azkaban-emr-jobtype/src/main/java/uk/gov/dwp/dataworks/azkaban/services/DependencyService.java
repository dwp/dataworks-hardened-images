package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DependencyService extends CancellableLoggingService implements MetadataService {

    public DependencyService(final AmazonDynamoDB dynamoDB, String metadataTableName, String exportDate) {
        this.dynamoDb = dynamoDB;
        this.metadataTableName = metadataTableName;
        this.exportDate = exportDate;
    }

    public Optional<Map<String, AttributeValue>> successfulDependency(final String product) {
        return proceed.get() ? dependencyMetadata(product).filter(this::dependencySucceeded)
                                                          .flatMap(x -> dependencyMetadata(product)) : Optional.empty();
    }

    @Override
    public void cancel() {
        super.cancel();
        this.latch.countDown();
    }

    private static Map<String, AttributeValue> primaryKey(final Map<String, AttributeValue> item) {
        return item.entrySet().stream().filter(entry -> entry.getKey().equals(CORRELATION_ID_FIELD) || entry.getKey()
                                                                                                            .equals(DATA_PRODUCT_FIELD))
                   .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Boolean dependencySucceeded(final Map<String, AttributeValue> item) {
        return completedSuccessfully(metadataTableName, item).orElse(false);
    }

    private Optional<Boolean> completedSuccessfully(final String tableName, final Map<String, AttributeValue> item) {
        try {
            final AtomicBoolean succeeded = new AtomicBoolean(false);
            final ScheduledExecutorService dependencyCheckExecutor = Executors.newSingleThreadScheduledExecutor();
            this.latch = new CountDownLatch(1);
            dependencyCheckExecutor
                    .scheduleWithFixedDelay(() -> checkDependency(tableName, item, succeeded), 0, pollIntervalMilliseconds(),
                            TimeUnit.MILLISECONDS);
            final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
            timeoutExecutor.schedule(latch::countDown, pollTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
            latch.await();
            timeoutExecutor.shutdownNow();
            dependencyCheckExecutor.shutdownNow();
            return Optional.of(succeeded.get());
        } catch (InterruptedException e) {
            error("Failed to check dependency '" + tableName + "', item: '" + item + "'", e);
            return Optional.empty();
        }
    }

    private void checkDependency(final String tableName, final Map<String, AttributeValue> item,
            final AtomicBoolean succeeded) {
        try {
            info("Checking '" + itemString(item) + "'");
            GetItemRequest request = new GetItemRequest().withTableName(tableName).withKey(primaryKey(item));
            GetItemResult result = dynamoDb.getItem(request);
            String status = result.getItem().get(STATUS_FIELD).getS();
            info("Checked '" + itemString(item) + "', status is '" + status + "'.");
            if (hasFinished(status)) {
                info("Dependency '" + itemString(item) + "' has completed, status is '" + status + "'.");
                succeeded.set(hasSucceeded(status));
                latch.countDown();
            }
        } catch (Exception e) {
            error("Failed to check item:", e);
        }
    }

    private Optional<Map<String, AttributeValue>> dependencyMetadata(final String product) {
        final List<Map<String, AttributeValue>> results = new ArrayList<>();
        Map<String, AttributeValue> lastKeyEvaluatedKey = null;
        do {
            ScanRequest request = scanRequest(product, lastKeyEvaluatedKey);
            ScanResult result = dynamoDb.scan(request);
            results.addAll(result.getItems());
            lastKeyEvaluatedKey = result.getLastEvaluatedKey();
        }
        while (lastKeyEvaluatedKey != null);

        return results.size() > 0 ? Optional.of(results.get(0)) : Optional.empty();
    }

    private static Map<String, AttributeValue> valueMap(final String product, final String exportDate) {
        Map<String, AttributeValue> valueMap = new HashMap<>();
        valueMap.put(":export_date", new AttributeValue().withS(exportDate));
        valueMap.put(":product", new AttributeValue().withS(product));
        return valueMap;
    }

    private static Map<String, String> nameMap() {
        Map<String, String> nameMap = new HashMap<>();
        nameMap.put("#export_date", DATE_FIELD);
        nameMap.put("#product", DATA_PRODUCT_FIELD);
        return nameMap;
    }

    private ScanRequest scanRequest(final String product, final Map<String, AttributeValue> lastKeyEvaluatedKey) {
        return new ScanRequest().withTableName(metadataTableName)
                                .withFilterExpression("#product = :product and #export_date = :export_date")
                                .withExclusiveStartKey(lastKeyEvaluatedKey).withExpressionAttributeNames(nameMap())
                                .withExpressionAttributeValues(valueMap(product, exportDate));
    }

    private static boolean hasFinished(final String status) {
        return hasSucceeded(status) || hasFailed(status);
    }

    private static boolean hasFailed(final String status) {
        return hasStatus(FAILED_COMPLETION_STATUS, status);
    }

    private static boolean hasSucceeded(final String status) {
        return hasStatus(SUCCESSFUL_COMPLETION_STATUS, status);
    }

    private static boolean hasStatus(final String required, final String status) {
        return required.equalsIgnoreCase(status);
    }

    private static String itemString(final Map<String, AttributeValue> item) {
        return item.get(CORRELATION_ID_FIELD).getS() + "/" + item.get(DATA_PRODUCT_FIELD).getS() + "/" + item
                .get(DATE_FIELD).getS();
    }

    private static int pollIntervalMilliseconds() {
        return Integer.parseInt(System.getProperty("poll.interval.milliseconds", "10000"));
    }

    private static int pollTimeoutMilliseconds() {
        return Integer.parseInt(System.getProperty("poll.timeout.milliseconds", "3600000"));
    }

    private final static String SUCCESSFUL_COMPLETION_STATUS = "Completed";
    private final static String FAILED_COMPLETION_STATUS = "Failed";
    private final String metadataTableName;
    private final String exportDate;

    private final AmazonDynamoDB dynamoDb;
    private final AtomicBoolean proceed = new AtomicBoolean(true);
    private CountDownLatch latch = new CountDownLatch(1);
}
