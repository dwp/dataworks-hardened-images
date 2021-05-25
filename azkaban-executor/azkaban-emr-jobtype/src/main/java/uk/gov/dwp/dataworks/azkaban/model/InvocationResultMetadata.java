package uk.gov.dwp.dataworks.azkaban.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class InvocationResultMetadata {

    private final static Integer SUCCESSFUL_RESPONSE_CODE = 200;
    private String requestId;
    private Integer statusCode;
    private Integer retryAttempts;

    public boolean wasSuccessful() {
        return SUCCESSFUL_RESPONSE_CODE.equals(statusCode);
    }

    @Override
    public String toString() {
        return "InvocationResultMetadata{" + "requestId='" + requestId + '\'' + ", statusCode=" + statusCode
                + ", retryAttempts=" + retryAttempts + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        InvocationResultMetadata that = (InvocationResultMetadata) o;
        return Objects.equals(requestId, that.requestId) && Objects.equals(statusCode, that.statusCode) && Objects
                .equals(retryAttempts, that.retryAttempts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, statusCode, retryAttempts);
    }

    @JsonProperty("RequestId")
    public String getRequestId() {
        return requestId;
    }

    @JsonProperty("RequestId")
    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    @JsonProperty("HTTPStatusCode")
    public int getStatusCode() {
        return statusCode;
    }

    @JsonProperty("HTTPStatusCode")
    public void setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
    }

    @JsonProperty("RetryAttempts")
    public Integer getRetryAttempts() {
        return retryAttempts;
    }

    @JsonProperty("RetryAttempts")
    public void setRetryAttempts(Integer retryAttempts) {
        this.retryAttempts = retryAttempts;
    }
}
