package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import uk.gov.dwp.dataworks.azkaban.model.InvocationPayload;
import uk.gov.dwp.dataworks.azkaban.model.InvocationResult;

import java.util.Optional;

public class LaunchInvocationService extends DelegateService {

    public LaunchInvocationService(final AWSLambda awsLambda, final String functionName) {
        this.awsLambda = awsLambda;
        this.functionName = functionName;
    }

    public Optional<InvocationResult> invokeEmrLauncher(final InvocationPayload payload) {
        if (proceed.get()) {
            try {
                info("Invoking lambda '" + functionName + "', payload: '" + payload + "'");
                InvokeResult result = awsLambda.invoke(invokeRequest(payload));
                info("Invoked lambda '" + functionName + "', payload: '" + payload + "', result: '" + result
                        .getStatusCode() + "'");
                String resultPayload = new String(result.getPayload().array());
                return Optional.of(new ObjectMapper().readValue(resultPayload, InvocationResult.class));
            } catch (Exception e) {
                error("Failed to invoke lambda launcher, function: " + functionName + "', payload: '" + payload
                        + "', message: '" + e.getMessage() + "'.", e);
                return Optional.empty();
            }
        } else {
            warn("Not invoking lambda, due to cancellation.");
            return Optional.empty();
        }
    }

    private InvokeRequest invokeRequest(InvocationPayload payload) throws JsonProcessingException {
        return new InvokeRequest().withFunctionName(this.functionName)
                                  .withPayload(new ObjectMapper().writeValueAsString(payload))
                                  .withInvocationType(InvocationType.RequestResponse);
    }

    private final AWSLambda awsLambda;
    private final String functionName;
}
