package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import uk.gov.dwp.dataworks.azkaban.model.InvocationPayload;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Used by the {@link EmrLaunchAndMonitoringService} to update dynamodb with entries
 * related to the current launch which indicate its status (started, succeeded, failed ...)
 */
public class StatusService implements MetadataService {

    private final String dataProduct;
    private final String metadataTableName;
    private final AmazonDynamoDB dynamoDb;
    private String correlationId;

    public StatusService(AmazonDynamoDB dynamoDB, String dataProduct, String metadataTableName) {
        this.dynamoDb = dynamoDB;
        this.dataProduct = dataProduct;
        this.metadataTableName = metadataTableName;
    }

    public void registerDependenciesCompleted(InvocationPayload payload) {
        try {
            this.correlationId = payload.getCorrelationId();
            Map<String, AttributeValue> item = new HashMap<>();
            item.put(CORRELATION_ID_FIELD, new AttributeValue().withS(payload.getCorrelationId()));
            item.put(DATA_PRODUCT_FIELD, new AttributeValue().withS(dataProduct()));
            item.put(DATE_FIELD, new AttributeValue().withS(payload.getExportDate()));

            Optional.ofNullable(payload.getSnapshotType()).ifPresent(
                    x -> item.put(SNAPSHOT_TYPE_FIELD, new AttributeValue().withS(payload.getSnapshotType())));

            Optional.ofNullable(payload.getAnalyticalDatasetPrefix()).ifPresent(x -> item
                    .put(ANALYTICAL_DATASET_PREFIX_FIELD,
                            new AttributeValue().withS(payload.getAnalyticalDatasetPrefix())));

            Optional.ofNullable(payload.getSnapshotPrefix()).ifPresent(
                    x -> item.put(SNAPSHOT_PREFIX_FIELD, new AttributeValue().withS(payload.getSnapshotPrefix())));

            item.put(STATUS_FIELD, new AttributeValue().withS("STARTED"));
            Calendar c = Calendar.getInstance();
            c.add(Calendar.WEEK_OF_YEAR, 1);
            item.put(TIME_TO_EXIST_FIELD, new AttributeValue().withN(String.format("%d", c.getTime().getTime())));
            final PutItemRequest request = new PutItemRequest().withTableName(metadataTableName).withItem(item);

            this.dynamoDb.putItem(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void registerClusterId(String clusterId) {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":cluster_id", new AttributeValue().withS(clusterId));
        UpdateItemRequest request = new UpdateItemRequest().withTableName(metadataTableName).withKey(recordIdentifier())
                                                           .withUpdateExpression(
                                                                   "set " + CLUSTER_ID_FIELD + " = :cluster_id")
                                                           .withExpressionAttributeValues(values);
        dynamoDb.updateItem(request);
    }

    public void registerSuccess() {
        registerStatus("SUCCEEDED");
    }

    public void registerFailure() {
        registerStatus("FAILED");
    }

    private void registerStatus(String status) {
        Map<String, AttributeValue> key = recordIdentifier();

        Map<String, String> names = new HashMap<>();
        names.put("#status_field", STATUS_FIELD);
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":status", new AttributeValue().withS(status));
        UpdateItemRequest request = new UpdateItemRequest().withTableName(metadataTableName).withKey(key)
                                                           .withUpdateExpression("set #status_field = :status")
                                                           .withExpressionAttributeNames(names)
                                                           .withExpressionAttributeValues(values);
        dynamoDb.updateItem(request);
    }

    private Map<String, AttributeValue> recordIdentifier() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(CORRELATION_ID_FIELD, new AttributeValue().withS(correlationId));
        key.put(DATA_PRODUCT_FIELD, new AttributeValue().withS(dataProduct()));
        return key;
    }

    private String dataProduct() {
        return "AZKABAN_" + dataProduct.trim().toUpperCase();
    }

}
