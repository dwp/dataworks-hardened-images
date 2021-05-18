package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import uk.gov.dwp.dataworks.azkaban.domain.InvocationPayload;
import uk.gov.dwp.dataworks.azkaban.domain.InvocationResult;
import uk.gov.dwp.dataworks.azkaban.domain.InvocationResultMetadata;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static uk.gov.dwp.dataworks.azkaban.services.PipelineMetadataService.CORRELATION_ID_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.PipelineMetadataService.DATE_FIELD;

class EmrLauncherLambdaServiceTest {

    @Test
    public void returnsPresentIfRequestSuccessful() throws JsonProcessingException {
        AWSLambda lambda = mock(AWSLambda.class);
        InvocationResultMetadata metadata = new InvocationResultMetadata();
        metadata.setStatusCode(200);
        InvocationResult result = new InvocationResult();
        result.setMetadata(metadata);
        InvokeResult invokeResult = new InvokeResult();
        invokeResult.setPayload(ByteBuffer.wrap(new ObjectMapper().writeValueAsBytes(result)));
        given(lambda.invoke(any())).willReturn(invokeResult);
        EmrLauncherLambdaService service = new EmrLauncherLambdaService(lambda, LAMBDA_NAME);
        Optional<InvocationResult> optionalResult = service.invokeEmrLauncher(payload());
        assertTrue(optionalResult.isPresent());
        optionalResult.ifPresent(x -> assertEquals(result, x));
    }

    @Test
    public void returnsEmptyIfRequestThrowsError() throws JsonProcessingException {
        AWSLambda lambda = mock(AWSLambda.class);
        InvocationResultMetadata metadata = new InvocationResultMetadata();
        metadata.setStatusCode(500);
        InvocationResult result = new InvocationResult();
        result.setMetadata(metadata);
        InvokeResult invokeResult = new InvokeResult();
        invokeResult.setPayload(ByteBuffer.wrap(new ObjectMapper().writeValueAsBytes(result)));
        given(lambda.invoke(any())).willThrow(new RuntimeException("FAILED"));
        EmrLauncherLambdaService service = new EmrLauncherLambdaService(lambda, LAMBDA_NAME);
        Optional<InvocationResult> optionalResult = service.invokeEmrLauncher(payload());
        assertTrue(optionalResult.isEmpty());
    }

    private InvocationPayload payload() {
        Map<String, AttributeValue> from = new HashMap<>();
        from.put(CORRELATION_ID_FIELD, attributeValue(CORRELATION_ID_VALUE));
        from.put(SNAPSHOT_TYPE_KEY, attributeValue(SNAPSHOT_TYPE_VALUE));
        from.put(DATE_FIELD, attributeValue(DATE_VALUE));
        from.put(S3_PREFIX_KEY, attributeValue(S3_PREFIX_VALUE));
        return InvocationPayload.from(from);
    }

    private AttributeValue attributeValue(String value) {
        return new AttributeValue().withS(value);
    }

    private final static String LAMBDA_NAME = "LAMBDA_NAME";
    private final static String SNAPSHOT_TYPE_KEY = "Snapshot_Type";
    private final static String S3_PREFIX_KEY = "S3_Prefix";

    private final static String CORRELATION_ID_VALUE = "CORRELATION_ID";
    private final static String DATE_VALUE = "DATE";
    private final static String S3_PREFIX_VALUE = "S3_PREFIX";
    private final static String SNAPSHOT_TYPE_VALUE = "SNAPSHOT_TYPE";
}
