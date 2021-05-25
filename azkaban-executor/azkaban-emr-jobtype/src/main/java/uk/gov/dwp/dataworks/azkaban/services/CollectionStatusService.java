package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Used by the {@link DependencyService} to poll the 'UCExportToCrownStatus' table until the entries with the given
 * collection names and correlation id have succeeded which is when their 'ADGStatus' is set to 'Completed'.
 * Has a timeout so it doesn't wait for ever.
 */
public class CollectionStatusService extends CancellableLoggingService {

    private final AmazonDynamoDB dynamoDb;
    private CountDownLatch latch = new CountDownLatch(1);
    public final static String EXPORT_STATUS_TABLE_NAME = "UCExportToCrownStatus";
    public final static String EXPORT_STATUS_CORRELATION_ID_FIELD = "CorrelationId";
    public final static String EXPORT_STATUS_COLLECTION_FIELD = "CollectionName";
    public final static String ADG_STATUS_FIELD = "ADGStatus";
    public final static String ADG_STATUS_COMPLETED = "Completed";

    public CollectionStatusService(AmazonDynamoDB dynamoDB) {
        this.dynamoDb = dynamoDB;
    }

    public Boolean collectionsSucceeded(String correlationId, final String ... collections) {
        return proceed.get() && Arrays.stream(collections)
                                      .allMatch(collection -> collectionSucceeded(correlationId, collection));
    }

    @Override
    public void cancel() {
        super.cancel();
        this.latch.countDown();
    }

    private Boolean collectionSucceeded(String correlationId, String collection) {
        if (proceed.get()) {
            final ScheduledExecutorService dependencyCheckExecutor = Executors.newSingleThreadScheduledExecutor();
            final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
            final AtomicBoolean succeeded = new AtomicBoolean(false);
            this.latch = new CountDownLatch(1);
            try {
                dependencyCheckExecutor.scheduleWithFixedDelay(() -> checkCollection(correlationId, collection, latch, succeeded), 0,
                        pollIntervalMilliseconds(), TimeUnit.MILLISECONDS);
                timeoutExecutor.schedule(latch::countDown, pollTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
                latch.await();
                return succeeded.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
                latch.countDown();
                return false;
            } finally {
                timeoutExecutor.shutdownNow();
                dependencyCheckExecutor.shutdownNow();
            }
        } else {
            return false;
        }
    }

    private void checkCollection(String correlationId, String collection, CountDownLatch latch, AtomicBoolean succeeded) {
        try {
            info("Checking '" + collection + "/" + correlationId + "', key " + primaryKey(correlationId, collection) + "'");
            GetItemRequest request = new GetItemRequest().withTableName(EXPORT_STATUS_TABLE_NAME).withKey(primaryKey(correlationId, collection));
            GetItemResult result = dynamoDb.getItem(request);
            info("Checked '" + collection + "/" + correlationId + "', result: '" + result.getItem() + "'");
            Optional.ofNullable(result.getItem())
                    .flatMap(item -> Optional.ofNullable(item.get(ADG_STATUS_FIELD)))
                    .map(AttributeValue::getS)
                    .filter(ADG_STATUS_COMPLETED::equalsIgnoreCase)
                    .ifPresent(status -> {
                        info("'" + collection + "/" + correlationId + "' has succeeded, status : '" + status + "'.");
                        succeeded.set(true);
                        latch.countDown();
                    });
        } catch (Exception e) {
            error("Failed to check '" + correlationId + "/" + collection + "': '" + e.getMessage() + "'", e);
        }
    }

    private Map<String, AttributeValue> primaryKey(String correlationId, String collection) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(EXPORT_STATUS_CORRELATION_ID_FIELD, new AttributeValue().withS(correlationId));
        key.put(EXPORT_STATUS_COLLECTION_FIELD, new AttributeValue().withS(collection));
        return key;
    }

    private static int pollIntervalMilliseconds() {
        return Integer.parseInt(System.getProperty("collection.status.poll.interval.milliseconds", "10000"));
    }

    private static int pollTimeoutMilliseconds() {
        return Integer.parseInt(System.getProperty("collection.status.poll.timeout.milliseconds", "18000000"));
    }
}
