package uk.gov.dwp.dataworks.azkaban.domain;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static uk.gov.dwp.dataworks.azkaban.services.PipelineMetadataService.CORRELATION_ID_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.PipelineMetadataService.DATE_FIELD;

public class LambdaPayload {

    public static LambdaPayload from(Map<String, AttributeValue> entry) {
        return new LambdaPayload(
                attributeValue(entry, CORRELATION_ID_FIELD),
                attributeValue(entry, "S3_Prefix_Analytical_DataSet", "S3_Prefix", "S3_Prefix_Snapshots"),
                attributeValue(entry, "Snapshot_Type"),
                attributeValue(entry, DATE_FIELD));
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
        LambdaPayload that = (LambdaPayload) o;
        return correlationId.equals(that.correlationId) && prefix.equals(that.prefix) && snapshotType
                .equals(that.snapshotType) && exportDate.equals(that.exportDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(correlationId, prefix, snapshotType, exportDate);
    }

    private LambdaPayload(String correlationId, String prefix, String snapshotType, String exportDate) {
        this.correlationId = correlationId;
        this.prefix = prefix;
        this.snapshotType = snapshotType;
        this.exportDate = exportDate;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getSnapshotType() {
        return snapshotType;
    }

    public String getExportDate() {
        return exportDate;
    }

    private static String attributeValue(Map<String, AttributeValue> map, String ... keys) {
        return Arrays.stream(keys).filter(map::containsKey)
                .map(map::get).map(AttributeValue::getS)
                .findFirst().orElse(DEFAULT_VALUE);
    }

    private final String correlationId;
    private final String prefix;
    private final String snapshotType;
    private final String exportDate;

    private final static String DEFAULT_VALUE = "NOT_SET";

}
