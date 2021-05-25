package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.dwp.dataworks.azkaban.services.DependencyService.CORRELATION_ID_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.DependencyService.DATA_PRODUCT_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.DependencyService.DATE_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.DependencyService.STATUS_FIELD;

class DataProductStatusServiceTest {

    @BeforeAll
    static void beforeAll() {
        System.setProperty("poll.timeout.milliseconds", "1000");
        System.setProperty("poll.interval.milliseconds", "10");
    }

    @Test
    public void shouldReturnSuccessfulItem() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        Map<String, AttributeValue> successfulItem = tableItem(STATUS_COMPLETED);
        Map<String, AttributeValue> successfulKey = itemKey();
        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(successfulKey);
        GetItemResult result = mock(GetItemResult.class);
        when(result.getItem()).thenReturn(successfulItem);
        when(dynamoDB.getItem(request)).thenReturn(result);
        DataProductStatusService service = new DataProductStatusService(dynamoDB, METADATA_TABLE);
        Boolean succeeded = service.dependencySucceeded(successfulItem);
        assertTrue(succeeded);
    }

    @Test
    public void shouldReturnEventuallySuccessfulItem() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        Map<String, AttributeValue> inProgressItem = tableItem(STATUS_PENDING);
        Map<String, AttributeValue> successfulItem = tableItem(STATUS_COMPLETED);
        Map<String, AttributeValue> itemKey = itemKey();
        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(itemKey);
        GetItemResult result = mock(GetItemResult.class);
        when(result.getItem()).thenReturn(inProgressItem).thenReturn(inProgressItem).thenReturn(inProgressItem)
                              .thenReturn(successfulItem);
        when(dynamoDB.getItem(request)).thenReturn(result);
        DataProductStatusService service = new DataProductStatusService(dynamoDB, METADATA_TABLE);
        Boolean succeeded = service.dependencySucceeded(successfulItem);
        assertTrue(succeeded);
        verify(dynamoDB, times(4)).getItem(request);
        verifyNoMoreInteractions(dynamoDB);
    }

    @Test
    public void shouldNotReturnFailedItem() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        Map<String, AttributeValue> failedItem = tableItem(STATUS_FAILED);
        Map<String, AttributeValue> successfulKey = itemKey();
        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(successfulKey);
        GetItemResult result = mock(GetItemResult.class);
        when(result.getItem()).thenReturn(failedItem);
        when(dynamoDB.getItem(request)).thenReturn(result);
        DataProductStatusService service = new DataProductStatusService(dynamoDB, METADATA_TABLE);
        Boolean succeeded = service.dependencySucceeded(failedItem);
        assertFalse(succeeded);
    }

    @Test
    public void shouldNotReturnTimedOutItem() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        Map<String, AttributeValue> pendingItem = tableItem(STATUS_PENDING);
        Map<String, AttributeValue> key = itemKey();
        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(key);
        GetItemResult result = mock(GetItemResult.class);
        when(result.getItem()).thenReturn(pendingItem);
        when(dynamoDB.getItem(request)).thenReturn(result);
        DataProductStatusService service = new DataProductStatusService(dynamoDB, METADATA_TABLE);
        Boolean succeeded = service.dependencySucceeded(pendingItem);
        assertFalse(succeeded);
    }

    @Test
    public void shouldNotReturnWhenCancelled() throws ExecutionException, InterruptedException {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        Map<String, AttributeValue> pendingItem = tableItem(STATUS_PENDING);
        Map<String, AttributeValue> successfulItem = tableItem(STATUS_COMPLETED);
        Map<String, AttributeValue> key = itemKey();

        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(key);

        GetItemResult result = mock(GetItemResult.class);
        when(result.getItem()).thenReturn(pendingItem).thenReturn(pendingItem).thenReturn(pendingItem)
                              .thenReturn(successfulItem);
        when(dynamoDB.getItem(request)).thenReturn(result);
        DataProductStatusService service = new DataProductStatusService(dynamoDB, METADATA_TABLE);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
        executorService.schedule(service::cancel, 15, TimeUnit.MILLISECONDS);
        Future<Boolean> future = executorService.submit(() -> service.dependencySucceeded(pendingItem));

        assertFalse(future.get());
    }

    private Map<String, AttributeValue> itemKey() {
        Map<String, AttributeValue> successfulKey = new HashMap<>();
        successfulKey.put(CORRELATION_ID_FIELD, new AttributeValue().withS(CORRELATION_ID_1));
        successfulKey.put(DATA_PRODUCT_FIELD, new AttributeValue().withS(PRODUCT_1));
        return successfulKey;
    }

    private Map<String, AttributeValue> tableItem(String status) {
        Map<String, AttributeValue> successfulItem = new HashMap<>();
        successfulItem.put(CORRELATION_ID_FIELD, new AttributeValue().withS(CORRELATION_ID_1));
        successfulItem.put(DATA_PRODUCT_FIELD, new AttributeValue().withS(PRODUCT_1));
        successfulItem.put(STATUS_FIELD, new AttributeValue().withS(status));
        successfulItem.put(DATE_FIELD, new AttributeValue().withS(EXPORT_DATE));
        return successfulItem;
    }

    private final static String METADATA_TABLE = "METADATA_TABLE";
    private final static String CORRELATION_ID_1 = "CORRELATION_ID_1";
    private final static String PRODUCT_1 = "PRODUCT_1";
    private final static String EXPORT_DATE = "2021-05-15";
    private final static String STATUS_COMPLETED = "Completed";
    private final static String STATUS_PENDING = "Pending";
    private final static String STATUS_FAILED = "Failed";
}
