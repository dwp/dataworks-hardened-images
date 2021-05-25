package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.dwp.dataworks.azkaban.services.CollectionStatusService.ADG_STATUS_COMPLETED;
import static uk.gov.dwp.dataworks.azkaban.services.CollectionStatusService.ADG_STATUS_FIELD;

class CollectionStatusServiceTest {

    @BeforeAll
    static void beforeAll() {
        System.setProperty("collection.status.poll.timeout.milliseconds", "1000");
        System.setProperty("collection.status.poll.interval.milliseconds", "10");
    }

    @Test
    public void shouldReturnTrueIfAllCollectionsTrue() {
        AmazonDynamoDB dynamoDb = mock(AmazonDynamoDB.class);
        GetItemResult result = mock(GetItemResult.class);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(ADG_STATUS_FIELD, new AttributeValue().withS(ADG_STATUS_COMPLETED));
        when(result.getItem()).thenReturn(item);
        when(dynamoDb.getItem(any())).thenReturn(result);
        CollectionStatusService service = new CollectionStatusService(dynamoDb);
        boolean succeeded = service.collectionsSucceeded(CORRELATION_ID, COLLECTION_1, COLLECTION_2, COLLECTION_3);
        assertTrue(succeeded);
    }

    @Test
    public void shouldFailAfterTimeout() {
        AmazonDynamoDB dynamoDb = mock(AmazonDynamoDB.class);
        GetItemResult result = mock(GetItemResult.class);
        Map<String, AttributeValue> successfulItem = new HashMap<>();
        successfulItem.put(ADG_STATUS_FIELD, new AttributeValue().withS(ADG_STATUS_COMPLETED));
        Map<String, AttributeValue> pendingItem = new HashMap<>();
        successfulItem.put(ADG_STATUS_FIELD, new AttributeValue().withS("Pending"));
        when(result.getItem()).thenReturn(successfulItem).thenReturn(successfulItem).thenReturn(pendingItem);
        when(dynamoDb.getItem(any())).thenReturn(result);
        CollectionStatusService service = new CollectionStatusService(dynamoDb);
        boolean succeeded = service.collectionsSucceeded(CORRELATION_ID, COLLECTION_1, COLLECTION_2, COLLECTION_3);
        assertFalse(succeeded);
    }

    @Test
    public void shouldFailIfCancelled() throws ExecutionException, InterruptedException {
        AmazonDynamoDB dynamoDb = mock(AmazonDynamoDB.class);
        GetItemResult result = mock(GetItemResult.class);
        Map<String, AttributeValue> successfulItem = new HashMap<>();
        successfulItem.put(ADG_STATUS_FIELD, new AttributeValue().withS(ADG_STATUS_COMPLETED));
        Map<String, AttributeValue> pendingItem = new HashMap<>();
        successfulItem.put(ADG_STATUS_FIELD, new AttributeValue().withS("Pending"));
        when(result.getItem()).thenReturn(pendingItem).thenReturn(pendingItem).thenReturn(successfulItem);
        when(dynamoDb.getItem(any())).thenReturn(result);

        CollectionStatusService service = new CollectionStatusService(dynamoDb);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
        executorService.schedule(service::cancel, 15, TimeUnit.MILLISECONDS);
        Future<Boolean> future = executorService.submit(() -> service.collectionsSucceeded(CORRELATION_ID, COLLECTION_1));
        assertFalse(future.get());
    }

    @Test
    public void shouldPollUntilSuccessful() {
        AmazonDynamoDB dynamoDb = mock(AmazonDynamoDB.class);
        GetItemResult pendingResult = mock(GetItemResult.class);
        GetItemResult successfulResult = mock(GetItemResult.class);
        Map<String, AttributeValue> successfulItem = new HashMap<>();
        successfulItem.put(ADG_STATUS_FIELD, new AttributeValue().withS(ADG_STATUS_COMPLETED));

        Map<String, AttributeValue> pendingItem = new HashMap<>();
        pendingItem.put(ADG_STATUS_FIELD, new AttributeValue().withS("Pending"));

        when(pendingResult.getItem()).thenReturn(pendingItem);
        when(successfulResult.getItem()).thenReturn(successfulItem);
        when(dynamoDb.getItem(any())).thenReturn(pendingResult, successfulResult);
        CollectionStatusService service = new CollectionStatusService(dynamoDb);
        boolean succeeded = service.collectionsSucceeded(CORRELATION_ID, COLLECTION_1);
        assertTrue(succeeded);
    }

    private final static String CORRELATION_ID = "CORRELATION_ID";
    private final static String COLLECTION_1 = "COLLECTION_1";
    private final static String COLLECTION_2 = "COLLECTION_2";
    private final static String COLLECTION_3 = "COLLECTION_3";

}
