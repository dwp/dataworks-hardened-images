package uk.gov.dwp.dataworks.azkaban.services;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.gov.dwp.dataworks.azkaban.model.InvocationPayload;
import uk.gov.dwp.dataworks.azkaban.model.InvocationResult;

import java.util.Optional;

public class EmrLauncherLambdaService extends CancellableService {

    public EmrLauncherLambdaService(final AWSLambda awsLambda, final String functionName) {
        this.awsLambda = awsLambda;
        this.functionName = functionName;
    }

    public Optional<InvocationResult> invokeEmrLauncher(final InvocationPayload payload) {
        if (proceed.get()) {
            try {
                logger.info("Invoking lambda '" + functionName + "', payload: '" + payload + "'");
                InvokeResult result = awsLambda.invoke(invokeRequest(payload));
                logger.info(
                        "Invoked lambda '" + functionName + "', payload: '" + payload + "', result: '" + result.getStatusCode() + "'");
                String resultPayload = new String(result.getPayload().array());
                return Optional.of(new ObjectMapper().readValue(resultPayload, InvocationResult.class));
            } catch (Exception e) {
                logger.error("Failed to invoke lambda launcher, function: " + functionName + "', payload: '" + payload
                        + "', message: '" + e.getMessage() + "'.", e);
                return Optional.empty();
            }
        } else {
            logger.warn("Not invoking lambda, due to cancellation.");
            return Optional.empty();
        }
    }

    private InvokeRequest invokeRequest(InvocationPayload payload) throws JsonProcessingException {
        return new InvokeRequest().withFunctionName(this.functionName)
                                  .withPayload(new ObjectMapper().writeValueAsString(payload))
                                  .withInvocationType(InvocationType.RequestResponse);
    }

    private final static Logger logger = LogManager.getLogger(EmrLauncherLambdaService.class);
    private final AWSLambda awsLambda;
    private final String functionName;
}
