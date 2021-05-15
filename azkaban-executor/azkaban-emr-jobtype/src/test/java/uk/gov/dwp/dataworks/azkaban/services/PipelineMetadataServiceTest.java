package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;
import static uk.gov.dwp.dataworks.azkaban.services.PipelineMetadataService.scanRequest;

class PipelineMetadataServiceTest {

    @BeforeAll
    static void beforeAll() {
        System.setProperty("poll.timeout.milliseconds", "1000");
        System.setProperty("poll.interval.milliseconds", "10");
    }

    @Test
    public void shouldReturnSuccessfulItem() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        ScanRequest scanRequest = scanRequest(METADATA_TABLE, PRODUCT_1, EXPORT_DATE, null);
        ScanResult scanResult = mock(ScanResult.class);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> successfulItem = tableItem(CORRELATION_ID_1, STATUS_COMPLETED);
        items.add(successfulItem);
        Map<String, AttributeValue> successfulKey = itemKey();

        given(scanResult.getItems()).willReturn(items);
        given(dynamoDB.scan(scanRequest)).willReturn(scanResult);
        given(scanResult.getLastEvaluatedKey()).willReturn(null);

        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(successfulKey);

        GetItemResult result = mock(GetItemResult.class);
        given(result.getItem()).willReturn(successfulItem);
        given(dynamoDB.getItem(request)).willReturn(result);

        PipelineMetadataService metadataService = new PipelineMetadataService(dynamoDB);
        Optional<List<Map<String, AttributeValue>>> successes = metadataService
                .successfulDependencies(METADATA_TABLE, EXPORT_DATE, PRODUCT_1);
        assertTrue(successes.isPresent());
        List<Map<String, AttributeValue>> item = successes.get();
        assertEquals(1, item.size());
        assertEquals(successfulItem, item.get(0));
    }

    @Test
    public void shouldReturnEventuallySuccessfulItem() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        ScanRequest scanRequest = scanRequest(METADATA_TABLE, PRODUCT_1, EXPORT_DATE, null);
        ScanResult scanResult = mock(ScanResult.class);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> inProgressItem = tableItem(CORRELATION_ID_1, STATUS_PENDING);
        items.add(inProgressItem);
        Map<String, AttributeValue> successfulItem = tableItem(CORRELATION_ID_1, STATUS_COMPLETED);
        List<Map<String, AttributeValue>> successfulItems = new ArrayList<>();
        successfulItems.add(successfulItem);
        Map<String, AttributeValue> itemKey = itemKey();

        given(scanResult.getItems()).willReturn(items).willReturn(successfulItems);
        given(dynamoDB.scan(scanRequest)).willReturn(scanResult);
        given(scanResult.getLastEvaluatedKey()).willReturn(null);

        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(itemKey);

        GetItemResult result = mock(GetItemResult.class);

        given(result.getItem()).willReturn(inProgressItem).willReturn(inProgressItem).willReturn(inProgressItem)
                .willReturn(successfulItem);

        given(dynamoDB.getItem(request)).willReturn(result);

        PipelineMetadataService metadataService = new PipelineMetadataService(dynamoDB);
        Optional<List<Map<String, AttributeValue>>> successes = metadataService
                .successfulDependencies(METADATA_TABLE, EXPORT_DATE, PRODUCT_1);
        assertTrue(successes.isPresent());
        List<Map<String, AttributeValue>> item = successes.get();
        assertEquals(1, item.size());
        assertEquals(successfulItem, item.get(0));
        verify(dynamoDB, times(4)).getItem(request);
        verify(dynamoDB, times(2)).scan(scanRequest);
        verifyNoMoreInteractions(dynamoDB);
    }

    @Test
    public void shouldNotReturnFailedItem() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        ScanRequest scanRequest = scanRequest(METADATA_TABLE, PRODUCT_1, EXPORT_DATE, null);
        ScanResult scanResult = mock(ScanResult.class);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> failedItem = tableItem(CORRELATION_ID_1, STATUS_FAILED);
        items.add(failedItem);
        Map<String, AttributeValue> successfulKey = itemKey();

        given(scanResult.getItems()).willReturn(items);
        given(dynamoDB.scan(scanRequest)).willReturn(scanResult);
        given(scanResult.getLastEvaluatedKey()).willReturn(null);

        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(successfulKey);

        GetItemResult result = mock(GetItemResult.class);
        given(result.getItem()).willReturn(failedItem);
        given(dynamoDB.getItem(request)).willReturn(result);

        PipelineMetadataService metadataService = new PipelineMetadataService(dynamoDB);
        Optional<List<Map<String, AttributeValue>>> successes = metadataService
                .successfulDependencies(METADATA_TABLE, EXPORT_DATE, PRODUCT_1);
        assertFalse(successes.isPresent());
    }

    @Test
    public void shouldNotReturnTimedOutItem() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        ScanRequest scanRequest = scanRequest(METADATA_TABLE, PRODUCT_1, EXPORT_DATE, null);
        ScanResult scanResult = mock(ScanResult.class);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> pendingItem = tableItem(CORRELATION_ID_1, STATUS_PENDING);
        items.add(pendingItem);
        Map<String, AttributeValue> key = itemKey();

        given(scanResult.getItems()).willReturn(items);
        given(dynamoDB.scan(scanRequest)).willReturn(scanResult);
        given(scanResult.getLastEvaluatedKey()).willReturn(null);

        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(key);

        GetItemResult result = mock(GetItemResult.class);
        given(result.getItem()).willReturn(pendingItem);
        given(dynamoDB.getItem(request)).willReturn(result);

        PipelineMetadataService metadataService = new PipelineMetadataService(dynamoDB);
        Optional<List<Map<String, AttributeValue>>> successes = metadataService
                .successfulDependencies(METADATA_TABLE, EXPORT_DATE, PRODUCT_1);
        assertFalse(successes.isPresent());
    }

    @Test
    public void shouldNotReturnWhenCancelled() throws ExecutionException, InterruptedException {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        ScanRequest scanRequest = scanRequest(METADATA_TABLE, PRODUCT_1, EXPORT_DATE, null);
        ScanResult scanResult = mock(ScanResult.class);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> pendingItem = tableItem(CORRELATION_ID_1, STATUS_PENDING);
        Map<String, AttributeValue> successfulItem = tableItem(CORRELATION_ID_1, STATUS_COMPLETED);
        items.add(pendingItem);
        Map<String, AttributeValue> key = itemKey();

        given(scanResult.getItems()).willReturn(items);
        given(dynamoDB.scan(scanRequest)).willReturn(scanResult);
        given(scanResult.getLastEvaluatedKey()).willReturn(null);

        GetItemRequest request = new GetItemRequest().withTableName(METADATA_TABLE).withKey(key);

        GetItemResult result = mock(GetItemResult.class);
        given(result.getItem()).willReturn(pendingItem).willReturn(pendingItem).willReturn(pendingItem).willReturn(successfulItem);
        given(dynamoDB.getItem(request)).willReturn(result);
        PipelineMetadataService metadataService = new PipelineMetadataService(dynamoDB);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
        executorService.schedule(metadataService::cancel, 15, TimeUnit.MILLISECONDS);
        Future<Optional<List<Map<String, AttributeValue>>>> f = executorService
                .submit(() -> metadataService.successfulDependencies(METADATA_TABLE, EXPORT_DATE, PRODUCT_1));
        assertFalse(f.get().isPresent());
    }

    @Test
    public void shouldPageForItems() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        List<Map<String, AttributeValue>> page1Items = new ArrayList<>();
        Map<String, AttributeValue> successfulItem1 = tableItem(CORRELATION_ID_1, STATUS_COMPLETED);
        page1Items.add(successfulItem1);
        ScanResult scanResult1 = mock(ScanResult.class);
        given(scanResult1.getItems()).willReturn(page1Items);
        given(scanResult1.getLastEvaluatedKey()).willReturn(itemKey());

        List<Map<String, AttributeValue>> page2Items = new ArrayList<>();
        Map<String, AttributeValue> successfulItem2 = tableItem(CORRELATION_ID_2, STATUS_COMPLETED);
        page2Items.add(successfulItem2);
        ScanResult scanResult2 = mock(ScanResult.class);
        given(scanResult2.getItems()).willReturn(page2Items);
        given(scanResult2.getLastEvaluatedKey()).willReturn(null);

        given(dynamoDB.scan(any(ScanRequest.class))).willReturn(scanResult1, scanResult2, scanResult1, scanResult2);

        GetItemResult result1 = mock(GetItemResult.class);
        given(result1.getItem()).willReturn(successfulItem1);
        GetItemResult result2 = mock(GetItemResult.class);
        given(result2.getItem()).willReturn(successfulItem2);
        given(dynamoDB.getItem(any(GetItemRequest.class))).willReturn(result1, result2);

        PipelineMetadataService metadataService = new PipelineMetadataService(dynamoDB);
        Optional<List<Map<String, AttributeValue>>> successes = metadataService
                .successfulDependencies(METADATA_TABLE, EXPORT_DATE, PRODUCT_1);
        assertTrue(successes.isPresent());
        List<Map<String, AttributeValue>> items = successes.get();
        assertEquals(2, items.size());
        assertEquals(successfulItem1, items.get(0));
        assertEquals(successfulItem2, items.get(1));
    }

    private Map<String, AttributeValue> itemKey() {
        Map<String, AttributeValue> successfulKey = new HashMap<>();
        successfulKey.put("Correlation_Id", new AttributeValue().withS(CORRELATION_ID_1));
        successfulKey.put("DataProduct", new AttributeValue().withS(PRODUCT_1));
        return successfulKey;
    }

    private Map<String, AttributeValue> tableItem(String correlationId, String status) {
        Map<String, AttributeValue> successfulItem = new HashMap<>();
        successfulItem.put("Correlation_Id", new AttributeValue().withS(correlationId));
        successfulItem.put("DataProduct", new AttributeValue().withS(PRODUCT_1));
        successfulItem.put("Status", new AttributeValue().withS(status));
        return successfulItem;
    }

    private final static String METADATA_TABLE = "METADATA_TABLE";
    private final static String CORRELATION_ID_1 = "CORRELATION_ID_1";
    private final static String CORRELATION_ID_2 = "CORRELATION_ID_2";
    private final static String PRODUCT_1 = "PRODUCT_1";
    private final static String EXPORT_DATE = "2021-05-15";
    private final static String STATUS_COMPLETED = "Completed";
    private final static String STATUS_PENDING = "Pending";
    private final static String STATUS_FAILED = "Failed";
}
