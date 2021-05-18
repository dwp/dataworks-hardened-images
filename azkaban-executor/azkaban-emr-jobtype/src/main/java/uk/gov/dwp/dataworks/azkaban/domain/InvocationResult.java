package uk.gov.dwp.dataworks.azkaban.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class InvocationResult {

    public boolean wasSuccessful() {
        return metadata != null && metadata.wasSuccessful();
    }

    @Override
    public String toString() {
        return "InvocationResult{" + "metadata=" + metadata + ", clusterId='" + clusterId + '\'' + ", clusterArn='"
                + clusterArn + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        InvocationResult that = (InvocationResult) o;
        return Objects.equals(metadata, that.metadata) && Objects.equals(clusterId, that.clusterId) && Objects
                .equals(clusterArn, that.clusterArn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadata, clusterId, clusterArn);
    }

    @JsonProperty("JobFlowId")
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    @JsonProperty("JobFlowId")
    public String getClusterId() {
        return clusterId;
    }

    @JsonProperty("ClusterArn")
    public String getClusterArn() {
        return clusterArn;
    }

    @JsonProperty("ClusterArn")
    public void setClusterArn(String clusterArn) {
        this.clusterArn = clusterArn;
    }

    @JsonProperty("ResponseMetadata")
    public InvocationResultMetadata getMetadata() {
        return metadata;
    }

    @JsonProperty("ResponseMetadata")
    public void setMetadata(InvocationResultMetadata metadata) {
        this.metadata = metadata;
    }

    private InvocationResultMetadata metadata;
    private String clusterId;
    private String clusterArn;
}
