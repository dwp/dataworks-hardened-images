package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Used by the {@link DependencyService} to determine when a data product has finished building and whether that build
 * succeeded.
 */
public class DataProductStatusService extends CancellableLoggingService implements MetadataService {

    private final static String SUCCESSFUL_COMPLETION_STATUS = "Completed";
    private final static String FAILED_COMPLETION_STATUS = "Failed";
    private final AmazonDynamoDB dynamoDb;
    private final CountDownLatch dependentProductLatch = new CountDownLatch(1);

    public DataProductStatusService(AmazonDynamoDB dynamoDB) {
        this.dynamoDb = dynamoDB;
    }

    public Boolean dependencySucceeded(final Map<String, AttributeValue> item) {
        return completedSuccessfully(item).orElse(false);
    }

    @Override
    public void cancel() {
        super.cancel();
        this.dependentProductLatch.countDown();
    }

    private Optional<Boolean> completedSuccessfully(final Map<String, AttributeValue> item) {
        final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
        final ScheduledExecutorService dependencyCheckExecutor = Executors.newSingleThreadScheduledExecutor();
        try {
            final AtomicBoolean succeeded = new AtomicBoolean(false);
            dependencyCheckExecutor.scheduleWithFixedDelay(() -> checkDependency(item, succeeded), 0,
                    pollIntervalMilliseconds(), TimeUnit.MILLISECONDS);
            timeoutExecutor
                    .schedule(dependentProductLatch::countDown, pollTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
            dependentProductLatch.await();
            return Optional.of(succeeded.get());
        } catch (InterruptedException e) {
            error("Failed to check dependency '" + PIPELINE_METADATA_TABLE + "', item: '" + item + "'", e);
            return Optional.empty();
        } finally {
            timeoutExecutor.shutdownNow();
            dependencyCheckExecutor.shutdownNow();
        }

    }

    private void checkDependency(final Map<String, AttributeValue> item,
            final AtomicBoolean succeeded) {
        try {
            info("Checking '" + itemString(item) + "'");
            final GetItemRequest request = new GetItemRequest().withTableName(PIPELINE_METADATA_TABLE).withKey(primaryKey(item));
            final GetItemResult result = dynamoDb.getItem(request);
            final String status = result.getItem().get(STATUS_FIELD).getS();
            info("Checked '" + itemString(item) + "', status is '" + status + "'.");
            if (hasFinished(status)) {
                info("Dependency '" + itemString(item) + "' has completed, status is '" + status + "'.");
                succeeded.set(hasSucceeded(status));
                dependentProductLatch.countDown();
            }
        } catch (Exception e) {
            error("Failed to check item:", e);
        }
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

    private static Map<String, AttributeValue> primaryKey(final Map<String, AttributeValue> item) {
        return item.entrySet().stream().filter(DataProductStatusService::isPrimaryKeyField)
                   .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static boolean isPrimaryKeyField(Map.Entry<String, AttributeValue> entry) {
        return entry.getKey().equals(CORRELATION_ID_FIELD) || entry.getKey().equals(DATA_PRODUCT_FIELD);
    }

    private static int pollIntervalMilliseconds() {
        return Integer.parseInt(System.getProperty("poll.interval.milliseconds", "10000"));
    }

    private static int pollTimeoutMilliseconds() {
        return Integer.parseInt(System.getProperty("poll.timeout.milliseconds", "18000000"));
    }
}
