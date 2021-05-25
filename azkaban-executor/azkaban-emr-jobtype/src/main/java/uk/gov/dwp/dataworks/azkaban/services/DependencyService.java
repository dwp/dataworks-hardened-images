package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Used by the {@link EmrLaunchAndMonitoringService} to check for the completion of the required
 * dependencies. This could be a data product such as 'ADG', or a list of collections. Depending on the type of
 * the dependency (data product or collection) this service delegates the checks to either a
 * {@link DataProductStatusService} or a {@link CollectionStatusService}
 */
public class DependencyService extends CancellableLoggingService implements MetadataService {

    private final String metadataTableName;
    private final String exportDate;
    private final AmazonDynamoDB dynamoDb;
    private final AtomicBoolean proceed = new AtomicBoolean(true);
    private final CountDownLatch dependentProductLatch = new CountDownLatch(1);
    private final CollectionStatusService collectionStatusService;
    private final DataProductStatusService dataProductStatusService;

    public DependencyService(final AmazonDynamoDB dynamoDB,
            final DataProductStatusService dataProductStatusService,
            final CollectionStatusService collectionStatusService,
            final String metadataTableName,
            final String exportDate) {
        this.dynamoDb = dynamoDB;
        this.collectionStatusService = collectionStatusService;
        this.dataProductStatusService = dataProductStatusService;
        this.metadataTableName = metadataTableName;
        this.exportDate = exportDate;
    }

    public Optional<Map<String, AttributeValue>> successfulDependency(final String product,
            final String... collections) {
        return proceed.get() ?
                dependencyMetadata(product).filter(collections.length > 0 ?
                        item -> collectionStatusService
                                .collectionsSucceeded(item.get(CORRELATION_ID_FIELD).getS(), collections) :
                        dataProductStatusService::dependencySucceeded).flatMap(x -> dependencyMetadata(product)) :
                Optional.empty();
    }

    private Optional<Map<String, AttributeValue>> dependencyMetadata(final String product) {
        final List<Map<String, AttributeValue>> results = new ArrayList<>();
        Map<String, AttributeValue> lastKeyEvaluatedKey = null;
        do {
            final ScanRequest request = scanRequest(product, lastKeyEvaluatedKey);
            final ScanResult result = dynamoDb.scan(request);
            results.addAll(result.getItems());
            lastKeyEvaluatedKey = result.getLastEvaluatedKey();
        }
        while (lastKeyEvaluatedKey != null);

        return results.size() > 0 ? Optional.of(results.get(0)) : Optional.empty();
    }

    private ScanRequest scanRequest(final String product, final Map<String, AttributeValue> lastKeyEvaluatedKey) {
        return new ScanRequest().withTableName(metadataTableName)
                                .withFilterExpression("#product = :product and #export_date = :export_date")
                                .withExclusiveStartKey(lastKeyEvaluatedKey).withExpressionAttributeNames(nameMap())
                                .withExpressionAttributeValues(valueMap(product, exportDate));
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

    @Override
    public void cancel() {
        super.cancel();
        this.dependentProductLatch.countDown();
    }
}
