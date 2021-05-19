package uk.gov.dwp.dataworks.azkaban.model;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static uk.gov.dwp.dataworks.azkaban.services.PipelineMetadataService.CORRELATION_ID_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.PipelineMetadataService.DATE_FIELD;

public class InvocationPayload {

    private final static String DEFAULT_VALUE = "NOT_SET";
    private final String correlationId;
    private final String prefix;
    private final String snapshotType;
    private final String exportDate;

    private InvocationPayload(String correlationId, String prefix, String snapshotType, String exportDate) {
        this.correlationId = correlationId;
        this.prefix = prefix;
        this.snapshotType = snapshotType;
        this.exportDate = exportDate;
    }

    public static InvocationPayload from(Map<String, AttributeValue> entry) {
        return new InvocationPayload(attributeValue(entry, CORRELATION_ID_FIELD),
                attributeValue(entry, "S3_Prefix_Analytical_DataSet", "S3_Prefix", "S3_Prefix_Snapshots"),
                attributeValue(entry, "Snapshot_Type"), attributeValue(entry, DATE_FIELD));
    }

    private static String attributeValue(Map<String, AttributeValue> map, String... keys) {
        return Arrays.stream(keys).filter(map::containsKey).map(map::get).map(AttributeValue::getS).findFirst()
                     .orElse(DEFAULT_VALUE);
    }

    @Override
    public String toString() {
        return "LambdaPayload{" + "correlationId='" + correlationId + '\'' + ", prefix='" + prefix + '\''
                + ", snapshotType='" + snapshotType + '\'' + ", exportDate='" + exportDate + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        InvocationPayload that = (InvocationPayload) o;
        return Objects.equals(correlationId, that.correlationId) && Objects.equals(prefix, that.prefix) && Objects
                .equals(snapshotType, that.snapshotType) && Objects.equals(exportDate, that.exportDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(correlationId, prefix, snapshotType, exportDate);
    }

    @JsonProperty("correlation_id")
    public String getCorrelationId() {
        return correlationId;
    }

    @JsonProperty("s3_prefix")
    public String getPrefix() {
        return prefix;
    }

    @JsonProperty("snapshot_type")
    public String getSnapshotType() {
        return snapshotType;
    }

    @JsonProperty("export_date")
    public String getExportDate() {
        return exportDate;
    }

}
