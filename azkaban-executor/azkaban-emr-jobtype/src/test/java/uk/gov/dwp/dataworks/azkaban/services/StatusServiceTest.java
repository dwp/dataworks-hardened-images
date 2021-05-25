package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import uk.gov.dwp.dataworks.azkaban.model.InvocationPayload;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static uk.gov.dwp.dataworks.azkaban.services.MetadataService.CORRELATION_ID_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.MetadataService.DATA_PRODUCT_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.MetadataService.PIPELINE_METADATA_TABLE;
import static uk.gov.dwp.dataworks.azkaban.services.MetadataService.STATUS_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.MetadataService.TIME_TO_EXIST_FIELD;

class StatusServiceTest {

    @Test
    public void shouldSetCorrectColumnValues() {
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
        StatusService statusService = new StatusService(dynamoDB, DATA_PRODUCT);
        Map<String, AttributeValue> dependenciesResult = new HashMap<>();
        dependenciesResult.put(CORRELATION_ID_FIELD, new AttributeValue().withS(CORRELATION_ID));
        InvocationPayload payload = InvocationPayload.from(dependenciesResult);
        statusService.registerDependenciesCompleted(payload);
        ArgumentCaptor<PutItemRequest> requestCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDB, times(1)).putItem(requestCaptor.capture());
        verifyNoMoreInteractions(dynamoDB);
        PutItemRequest actualPutRequest = requestCaptor.getValue();
        assertEquals(PIPELINE_METADATA_TABLE, actualPutRequest.getTableName());
        Map<String, AttributeValue> item = actualPutRequest.getItem();
        assertEquals(CORRELATION_ID, item.get(CORRELATION_ID_FIELD).getS());
        assertEquals("AZKABAN_" + DATA_PRODUCT, item.get(DATA_PRODUCT_FIELD).getS());
        assertEquals("STARTED", item.get(STATUS_FIELD).getS());
        assertNotNull(item.get(TIME_TO_EXIST_FIELD).getN());

        reset(dynamoDB);

        ArgumentCaptor<UpdateItemRequest> updateCaptor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        statusService.registerClusterId(CLUSTER_ID);
        verify(dynamoDB, times(1)).updateItem(updateCaptor.capture());
        verifyNoMoreInteractions(dynamoDB);
        UpdateItemRequest actualUpdateRequest = updateCaptor.getValue();
        assertEquals(PIPELINE_METADATA_TABLE, actualUpdateRequest.getTableName());
        Map<String, AttributeValue> requiredKey = new HashMap<>();
        requiredKey.put(CORRELATION_ID_FIELD, new AttributeValue().withS(CORRELATION_ID));
        requiredKey.put(DATA_PRODUCT_FIELD, new AttributeValue().withS("AZKABAN_" + DATA_PRODUCT));
        assertEquals(requiredKey, actualUpdateRequest.getKey());
        Map<String, AttributeValue> requiredValue = new HashMap<>();
        requiredValue.put(":cluster_id", new AttributeValue().withS(CLUSTER_ID));
        assertEquals(requiredValue, actualUpdateRequest.getExpressionAttributeValues());

        reset(dynamoDB);

        ArgumentCaptor<UpdateItemRequest> statusCaptor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        statusService.registerSuccess();
        verify(dynamoDB, times(1)).updateItem(statusCaptor.capture());
        verifyNoMoreInteractions(dynamoDB);
        UpdateItemRequest successRequest = statusCaptor.getValue();
        assertEquals(PIPELINE_METADATA_TABLE, successRequest.getTableName());
        assertEquals(requiredKey, successRequest.getKey());
        Map<String, AttributeValue> successValue = new HashMap<>();
        successValue.put(":status", new AttributeValue().withS(SUCCEEDED_STATUS));
        assertEquals(successValue, successRequest.getExpressionAttributeValues());

        reset(dynamoDB);

        ArgumentCaptor<UpdateItemRequest> failedStatusCaptor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        statusService.registerFailure();
        verify(dynamoDB, times(1)).updateItem(failedStatusCaptor.capture());
        verifyNoMoreInteractions(dynamoDB);
        UpdateItemRequest failureRequest = failedStatusCaptor.getValue();
        assertEquals(PIPELINE_METADATA_TABLE, failureRequest.getTableName());
        assertEquals(requiredKey, failureRequest.getKey());
        Map<String, AttributeValue> failureValue = new HashMap<>();
        failureValue.put(":status", new AttributeValue().withS(FAILED_STATUS));
        assertEquals(failureValue, failureRequest.getExpressionAttributeValues());
    }

    private final static String DATA_PRODUCT = "DATA_PRODUCT";
    private final static String CLUSTER_ID = "CLUSTER_ID";
    private final static String SUCCEEDED_STATUS = "SUCCEEDED";
    private final static String FAILED_STATUS = "FAILED";
    private final static String CORRELATION_ID = "CORRELATION_ID";
}
