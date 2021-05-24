package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
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

public class PipelineMetadataService extends EmrLaunchingDelegateService {

    public PipelineMetadataService(final AmazonDynamoDB dynamoDB, String dataProduct, String metadataTableName,
            String exportDate) {
        this.dynamoDb = dynamoDB;
        this.dataProduct = dataProduct;
        this.metadataTableName = metadataTableName;
        this.exportDate = exportDate;
    }

    public Optional<List<Map<String, AttributeValue>>> successfulDependencies(final String ... products) {
        System.out.println("WOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
        return proceed.get() && dependenciesSucceeded(dependenciesMetadata(products)) ?
                Optional.of(dependenciesMetadata(products)) :
                Optional.empty();
    }

    public void registerStarted(String correlationId) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(CORRELATION_ID_FIELD, new AttributeValue().withS(correlationId));
        item.put(DATA_PRODUCT_FIELD, new AttributeValue().withS("AZKABAN_" + dataProduct.trim().toUpperCase()));
        item.put(DATE_FIELD, new AttributeValue().withS(exportDate));
        item.put(STATUS_FIELD, new AttributeValue().withS("STARTING"));
        Calendar c = Calendar.getInstance();
        c.add(Calendar.WEEK_OF_YEAR, 1);
        item.put(TIME_TO_EXIST_FIELD, new AttributeValue().withN(String.format("%d", c.getTime().getTime())));
        info("Putting new item '" + item + "'.");
        final PutItemRequest request = new PutItemRequest().withTableName(metadataTableName).withItem(item);

        PutItemResult result = this.dynamoDb.putItem(request);
        info("Put new item '" + item + "', result: " + result.getAttributes() + "'");
    }

    //    public void registerInvoked(final String tableName, final String exportDate, final String correlationId) {
    //        Map<String, AttributeValue> key = new HashMap<>();
    //        key.put(CORRELATION_ID_FIELD, new AttributeValue().withS(temporaryCorrelationId));
    //        key.put(DATA_PRODUCT_FIELD, new AttributeValue().withS(dataProduct));
    //
    //
    //        Map<String, AttributeValue> values = new HashMap<>();
    //        values.put(":correlation_id", new AttributeValue().withS(correlationId));
    //        UpdateItemRequest request =
    //                new UpdateItemRequest().withTableName(tableName)
    //                                       .withKey(key)
    //                                       .withUpdateExpression("set " + CORRELATION_ID_FIELD + " = :correlation_id")
    //                                       .withExpressionAttributeValues(values);
    //        UpdateItemResult result = dynamoDb.updateItem(request);
    //        System.out.println("RESULT: '" + result + "'");
    //    }

    public ScanRequest scanRequest(final String product, final Map<String, AttributeValue> lastKeyEvaluatedKey) {
        return new ScanRequest().withTableName(metadataTableName)
                                .withFilterExpression("#product = :product and #export_date = :export_date")
                                .withExclusiveStartKey(lastKeyEvaluatedKey).withExpressionAttributeNames(nameMap())
                                .withExpressionAttributeValues(valueMap(product, exportDate));
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

    private List<Map<String, AttributeValue>> dependenciesMetadata(final String... products) {
        return Arrays.stream(products).map(this::dependencyMetadata)
                     .reduce(new ArrayList<>(), (accumulation, next) -> {
                         accumulation.addAll(next);
                         return accumulation;
                     });
    }

    private Boolean dependenciesSucceeded(final List<Map<String, AttributeValue>> items) {
        return items.stream().map(item -> completedSuccessfully(metadataTableName, item))
                    .filter(x -> !x.isPresent() || !x.get()).map(Optional::get)
                    .reduce(true, (acc, next) -> acc && next);
    }

    private Optional<Boolean> completedSuccessfully(final String tableName, final Map<String, AttributeValue> item) {
        try {
            final AtomicBoolean succeeded = new AtomicBoolean(false);
            final ScheduledExecutorService dependencyCheckExecutor = Executors.newSingleThreadScheduledExecutor();
            this.latch = new CountDownLatch(1);
            dependencyCheckExecutor
                    .scheduleWithFixedDelay(() -> checkDependency(tableName, item, succeeded), 0, pollIntervalSeconds(),
                            TimeUnit.MILLISECONDS);
            final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
            timeoutExecutor.schedule(latch::countDown, pollTimeoutSeconds(), TimeUnit.MILLISECONDS);
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

    private List<Map<String, AttributeValue>> dependencyMetadata(final String product) {
        final List<Map<String, AttributeValue>> results = new ArrayList<>();
        Map<String, AttributeValue> lastKeyEvaluatedKey = null;
        do {
            ScanRequest request = scanRequest(product, lastKeyEvaluatedKey);
            ScanResult result = dynamoDb.scan(request);
            results.addAll(result.getItems());
            lastKeyEvaluatedKey = result.getLastEvaluatedKey();
        }
        while (lastKeyEvaluatedKey != null);

        return results;
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

    private static int pollIntervalSeconds() {
        return Integer.parseInt(System.getProperty("poll.interval.milliseconds", "10000"));
    }

    private static int pollTimeoutSeconds() {
        return Integer.parseInt(System.getProperty("poll.timeout.milliseconds", "3600000"));
    }

    public final static String CORRELATION_ID_FIELD = "Correlation_Id";
    public final static String DATA_PRODUCT_FIELD = "DataProduct";
    public final static String TIME_TO_EXIST_FIELD = "TimeToExist";
    public final static String STATUS_FIELD = "Status";
    public final static String DATE_FIELD = "Date";
    private final static String SUCCESSFUL_COMPLETION_STATUS = "Completed";
    private final static String FAILED_COMPLETION_STATUS = "Failed";
    private final String dataProduct;
    private final String metadataTableName;
    private final String exportDate;

    private final AmazonDynamoDB dynamoDb;
    private final AtomicBoolean proceed = new AtomicBoolean(true);
    private CountDownLatch latch = new CountDownLatch(1);
}
