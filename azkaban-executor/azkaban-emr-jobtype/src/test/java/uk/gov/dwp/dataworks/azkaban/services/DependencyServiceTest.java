package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.dwp.dataworks.azkaban.services.DependencyService.CORRELATION_ID_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.DependencyService.DATA_PRODUCT_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.DependencyService.DATE_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.DependencyService.STATUS_FIELD;

class DependencyServiceTest {

    @BeforeAll
    static void beforeAll() {
        System.setProperty("poll.timeout.milliseconds", "1000");
        System.setProperty("poll.interval.milliseconds", "10");
    }

    @Test
    public void shouldReturnSuccessfulItem() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        ScanResult scanResult = mock(ScanResult.class);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> successfulItem = tableItem(CORRELATION_ID_1, STATUS_COMPLETED);
        items.add(successfulItem);
        Map<String, AttributeValue> successfulKey = itemKey();

        when(scanResult.getItems()).thenReturn(items);
        when(dynamoDB.scan(any())).thenReturn(scanResult);
        when(scanResult.getLastEvaluatedKey()).thenReturn(null);

        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(successfulKey);

        GetItemResult result = mock(GetItemResult.class);
        when(result.getItem()).thenReturn(successfulItem);
        when(dynamoDB.getItem(request)).thenReturn(result);

        DependencyService metadataService = new DependencyService(dynamoDB, METADATA_TABLE, EXPORT_STATUS_TABLE, EXPORT_DATE);
        Optional<Map<String, AttributeValue>> successes = metadataService.successfulDependency(PRODUCT_1);
        assertTrue(successes.isPresent());
        Map<String, AttributeValue> item = successes.get();
        assertEquals(successfulItem, item);
    }

    @Test
    public void shouldReturnEventuallySuccessfulItem() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        ScanResult scanResult = mock(ScanResult.class);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> inProgressItem = tableItem(CORRELATION_ID_1, STATUS_PENDING);
        items.add(inProgressItem);
        Map<String, AttributeValue> successfulItem = tableItem(CORRELATION_ID_1, STATUS_COMPLETED);
        List<Map<String, AttributeValue>> successfulItems = new ArrayList<>();
        successfulItems.add(successfulItem);
        Map<String, AttributeValue> itemKey = itemKey();

        when(scanResult.getItems()).thenReturn(items).thenReturn(successfulItems);
        when(dynamoDB.scan(any())).thenReturn(scanResult);
        when(scanResult.getLastEvaluatedKey()).thenReturn(null);

        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(itemKey);

        GetItemResult result = mock(GetItemResult.class);

        when(result.getItem()).thenReturn(inProgressItem).thenReturn(inProgressItem).thenReturn(inProgressItem)
                              .thenReturn(successfulItem);

        when(dynamoDB.getItem(request)).thenReturn(result);

        DependencyService metadataService = new DependencyService(dynamoDB, METADATA_TABLE, EXPORT_STATUS_TABLE, EXPORT_DATE);
        Optional<Map<String, AttributeValue>> successes = metadataService.successfulDependency(PRODUCT_1);
        assertTrue(successes.isPresent());
        Map<String, AttributeValue> item = successes.get();
        assertEquals(successfulItem, item);
        verify(dynamoDB, times(4)).getItem(request);
        verify(dynamoDB, times(2)).scan(any());
        verifyNoMoreInteractions(dynamoDB);
    }

    @Test
    public void shouldNotReturnFailedItem() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        ScanResult scanResult = mock(ScanResult.class);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> failedItem = tableItem(CORRELATION_ID_1, STATUS_FAILED);
        items.add(failedItem);
        Map<String, AttributeValue> successfulKey = itemKey();

        when(scanResult.getItems()).thenReturn(items);
        when(dynamoDB.scan(any())).thenReturn(scanResult);
        when(scanResult.getLastEvaluatedKey()).thenReturn(null);

        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(successfulKey);

        GetItemResult result = mock(GetItemResult.class);
        when(result.getItem()).thenReturn(failedItem);
        when(dynamoDB.getItem(request)).thenReturn(result);

        DependencyService metadataService = new DependencyService(dynamoDB, METADATA_TABLE, EXPORT_STATUS_TABLE, EXPORT_DATE);
        Optional<Map<String, AttributeValue>> successes = metadataService.successfulDependency(PRODUCT_1);
        assertFalse(successes.isPresent());
    }

    @Test
    public void shouldNotReturnTimedOutItem() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        ScanResult scanResult = mock(ScanResult.class);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> pendingItem = tableItem(CORRELATION_ID_1, STATUS_PENDING);
        items.add(pendingItem);
        Map<String, AttributeValue> key = itemKey();

        when(scanResult.getItems()).thenReturn(items);
        when(dynamoDB.scan(any())).thenReturn(scanResult);
        when(scanResult.getLastEvaluatedKey()).thenReturn(null);

        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(key);

        GetItemResult result = mock(GetItemResult.class);
        when(result.getItem()).thenReturn(pendingItem);
        when(dynamoDB.getItem(request)).thenReturn(result);

        DependencyService metadataService = new DependencyService(dynamoDB, METADATA_TABLE, EXPORT_STATUS_TABLE, EXPORT_DATE);
        Optional<Map<String, AttributeValue>> successes = metadataService.successfulDependency(PRODUCT_1);
        assertFalse(successes.isPresent());
    }

    @Test
    public void shouldNotReturnWhenCancelled() throws ExecutionException, InterruptedException {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        ScanResult scanResult = mock(ScanResult.class);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> pendingItem = tableItem(CORRELATION_ID_1, STATUS_PENDING);
        Map<String, AttributeValue> successfulItem = tableItem(CORRELATION_ID_1, STATUS_COMPLETED);
        items.add(pendingItem);
        Map<String, AttributeValue> key = itemKey();

        when(scanResult.getItems()).thenReturn(items);
        when(dynamoDB.scan(any())).thenReturn(scanResult);
        when(scanResult.getLastEvaluatedKey()).thenReturn(null);

        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(key);

        GetItemResult result = mock(GetItemResult.class);
        when(result.getItem()).thenReturn(pendingItem).thenReturn(pendingItem).thenReturn(pendingItem)
                              .thenReturn(successfulItem);
        when(dynamoDB.getItem(request)).thenReturn(result);
        DependencyService metadataService = new DependencyService(dynamoDB, METADATA_TABLE, EXPORT_STATUS_TABLE, EXPORT_DATE);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
        executorService.schedule(metadataService::cancel, 15, TimeUnit.MILLISECONDS);
        Future<Optional<Map<String, AttributeValue>>> future = executorService
                .submit(() -> metadataService.successfulDependency(PRODUCT_1));

        assertFalse(future.get().isPresent());
    }

    @Test
    public void shouldPageForItems() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        List<Map<String, AttributeValue>> page1Items = new ArrayList<>();
        Map<String, AttributeValue> successfulItem1 = tableItem(CORRELATION_ID_1, STATUS_COMPLETED);
        page1Items.add(successfulItem1);
        ScanResult scanResult1 = mock(ScanResult.class);
        when(scanResult1.getItems()).thenReturn(page1Items);
        when(scanResult1.getLastEvaluatedKey()).thenReturn(itemKey());

        List<Map<String, AttributeValue>> page2Items = new ArrayList<>();
        Map<String, AttributeValue> successfulItem2 = tableItem(CORRELATION_ID_2, STATUS_COMPLETED);
        page2Items.add(successfulItem2);
        ScanResult scanResult2 = mock(ScanResult.class);
        when(scanResult2.getItems()).thenReturn(page2Items);
        when(scanResult2.getLastEvaluatedKey()).thenReturn(null);

        when(dynamoDB.scan(any(ScanRequest.class))).thenReturn(scanResult1, scanResult2, scanResult1, scanResult2);

        GetItemResult result1 = mock(GetItemResult.class);
        when(result1.getItem()).thenReturn(successfulItem1);
        GetItemResult result2 = mock(GetItemResult.class);
        when(result2.getItem()).thenReturn(successfulItem2);
        when(dynamoDB.getItem(any(GetItemRequest.class))).thenReturn(result1, result2);

        DependencyService metadataService = new DependencyService(dynamoDB, METADATA_TABLE, EXPORT_STATUS_TABLE, EXPORT_DATE);
        Optional<Map<String, AttributeValue>> successes = metadataService
                .successfulDependency(PRODUCT_1);
        assertTrue(successes.isPresent());
        Map<String, AttributeValue> item = successes.get();
        assertEquals(successfulItem1, item);
    }

    private Map<String, AttributeValue> itemKey() {
        Map<String, AttributeValue> successfulKey = new HashMap<>();
        successfulKey.put(CORRELATION_ID_FIELD, new AttributeValue().withS(CORRELATION_ID_1));
        successfulKey.put(DATA_PRODUCT_FIELD, new AttributeValue().withS(PRODUCT_1));
        return successfulKey;
    }

    private Map<String, AttributeValue> tableItem(String correlationId, String status) {
        Map<String, AttributeValue> successfulItem = new HashMap<>();
        successfulItem.put(CORRELATION_ID_FIELD, new AttributeValue().withS(correlationId));
        successfulItem.put(DATA_PRODUCT_FIELD, new AttributeValue().withS(PRODUCT_1));
        successfulItem.put(STATUS_FIELD, new AttributeValue().withS(status));
        successfulItem.put(DATE_FIELD, new AttributeValue().withS(EXPORT_DATE));
        return successfulItem;
    }

    private final static String METADATA_TABLE = "METADATA_TABLE";
    private final static String EXPORT_STATUS_TABLE = "METADATA_TABLE";
    private final static String CORRELATION_ID_1 = "CORRELATION_ID_1";
    private final static String CORRELATION_ID_2 = "CORRELATION_ID_2";
    private final static String PRODUCT_1 = "PRODUCT_1";
    private final static String EXPORT_DATE = "2021-05-15";
    private final static String STATUS_COMPLETED = "Completed";
    private final static String STATUS_PENDING = "Pending";
    private final static String STATUS_FAILED = "Failed";
}
