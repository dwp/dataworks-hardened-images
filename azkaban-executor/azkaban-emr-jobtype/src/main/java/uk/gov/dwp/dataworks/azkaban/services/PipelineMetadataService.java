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
        return successfulDependencies(tableName, dependenciesMetadata(tableName, exportDate, products)) ?
            Optional.of(dependenciesMetadata(tableName, exportDate, products)) : Optional.empty();
    }

    private List<Map<String, AttributeValue>> dependenciesMetadata(String tableName, String exportDate,
            String... products) {
        return Arrays.stream(products).map(product -> dependenciesMetadata(tableName, product, exportDate))
                .reduce(new ArrayList<>(), (accumulation, next) -> {
                    accumulation.addAll(next);
                    return accumulation;
                });
    }

    public boolean successfulDependencies(String tableName, List<Map<String, AttributeValue>> items) {
        return items.stream().map(item -> completedSuccessfully(tableName, item)).filter(x -> !x).findFirst().orElse(true);
    }

    public boolean completedSuccessfully(String tableName, Map<String, AttributeValue> item) {
        try {
            final AtomicBoolean succeeded = new AtomicBoolean(false);
            final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            final CountDownLatch latch = new CountDownLatch(1);
            executor.scheduleAtFixedRate(() -> {
                logger.info("Looking up '" + itemString(item) + "'");
                GetItemRequest request = new GetItemRequest().withTableName(tableName).withKey(primaryKey(item));
                GetItemResult result = dynamoDb.getItem(request);
                String status = result.getItem().get(STATUS_FIELD).getS();
                logger.info("Looked up '" + itemString(item) + "', status is '" + status + "'");
                if (status.equals("Completed") || status.equals("Failed")) {
                    succeeded.set(status.equals("Completed"));
                    latch.countDown();
                }
            }, 0, 10, TimeUnit.SECONDS);
            final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
            timeoutExecutor.schedule(latch::countDown, 10, TimeUnit.SECONDS);
            latch.await();
            timeoutExecutor.shutdownNow();
            executor.shutdownNow();
            return succeeded.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, AttributeValue> primaryKey(Map<String, AttributeValue> item) {
        return item.entrySet().stream()
                .filter(entry -> entry.getKey().equals(PARTITION_KEY_FIELD) || entry.getKey()
                        .equals(SORT_KEY_FIELD)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private List<Map<String, AttributeValue>> dependenciesMetadata(String tableName, String product,
            String exportDate) {
        List<Map<String, AttributeValue>> results = new ArrayList<>();
        Map<String, String> nameMap = new HashMap<>();
        nameMap.put("#export_date", "Date");
        nameMap.put("#product", "DataProduct");
        Map<String, AttributeValue> valueMap = new HashMap<>();
        valueMap.put(":export_date", new AttributeValue().withS(exportDate));
        valueMap.put(":product", new AttributeValue().withS(product));
        Map<String, AttributeValue> lastKeyEvaluatedKey = null;
        do {
            ScanRequest request = new ScanRequest().withTableName(tableName)
                    .withFilterExpression("#product = :product and #export_date = :export_date")
                    .withExclusiveStartKey(lastKeyEvaluatedKey).withExpressionAttributeNames(nameMap)
                    .withExpressionAttributeValues(valueMap);
            ScanResult result = dynamoDb.scan(request);
            results.addAll(result.getItems());
            lastKeyEvaluatedKey = result.getLastEvaluatedKey();
        } while (lastKeyEvaluatedKey != null);

        return results;
    }

    private String itemString(Map<String, AttributeValue> item) {
        return item.get(PARTITION_KEY_FIELD).getS() + "/" + item.get(SORT_KEY_FIELD).getS() + "/" + item.get(DATE_FIELD);
    }

    private final AmazonDynamoDB dynamoDb;
    private final static String PARTITION_KEY_FIELD = "Correlation_Id";
    private final static String SORT_KEY_FIELD = "DataProduct";
    private final static String STATUS_FIELD = "Status";
    private final static String DATE_FIELD = "Date";
    private final static String COMPLETED_STATUS_VALUE = "Completed";
    private final static Logger logger = LoggerFactory.getLogger(PipelineMetadataService.class);
}
