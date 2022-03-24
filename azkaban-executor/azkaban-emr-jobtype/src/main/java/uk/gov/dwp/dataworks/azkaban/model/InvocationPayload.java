package uk.gov.dwp.dataworks.azkaban.model;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

import static uk.gov.dwp.dataworks.azkaban.services.DependencyService.CORRELATION_ID_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.DependencyService.DATE_FIELD;

public class InvocationPayload {

    private final static String DEFAULT_VALUE = "NOT_SET";
    private final String correlationId;
    private final String analyticalDatasetPrefix;
    private final String snapshotPrefix;
    private final String snapshotType;
    private final String exportDate;

    private InvocationPayload(String correlationId, String analyticalDatasetPrefix, String snapshotPrefix,
            String snapshotType, String exportDate) {
        this.correlationId = correlationId;
        this.analyticalDatasetPrefix = analyticalDatasetPrefix;
        this.snapshotPrefix = snapshotPrefix;
        this.snapshotType = snapshotType;
        this.exportDate = exportDate;
    }

    public static InvocationPayload from(Map<String, AttributeValue> entry) {
        return new InvocationPayload(attributeValue(entry, CORRELATION_ID_FIELD),
                attributeValue(entry, "S3_Prefix_Analytical_DataSet"), attributeValue(entry, "S3_Prefix_Snapshots"),
                attributeValue(entry, "Snapshot_Type"), attributeValue(entry, DATE_FIELD));
    }

    public Map<String, Map<String, ArrayList<String>>> getPayload() {

        Map<String, Map<String, ArrayList<String>>> res = new HashMap<>();
        Map<String, ArrayList<String>> values = new HashMap<>();
        ArrayList<String> list = new ArrayList<>();

        list.add("--correlation_id");
        list.add(correlationId);
        list.add("--s3_prefix");
        list.add(snapshotPrefix);
        list.add("--snapshot_type");
        list.add(snapshotType);
        list.add("--export_date");
        list.add(exportDate);

        values.put("submit-job", list);
        values.put("courtesy-flush", list);
        values.put("send_notification", list);
        values.put("create-clive-databases", list);
        values.put("source", list);
        values.put("create_uc_feature_dbs", list);
        values.put("create-hive-dynamo-table", list);

        res.put("additional_step_args", values);
        return res;
    }

    private static String attributeValue(Map<String, AttributeValue> map, String... keys) {
        return Arrays.stream(keys).filter(map::containsKey).map(map::get).map(AttributeValue::getS).findFirst()
                     .orElse(DEFAULT_VALUE);
    }

    @Override
    public String toString() {
        return "InvocationPayload{" + "correlationId='" + correlationId + '\'' + ", analyticalDatasetPrefix='"
                + analyticalDatasetPrefix + '\'' + ", snapshotPrefix='" + snapshotPrefix + '\'' + ", snapshotType='"
                + snapshotType + '\'' + ", exportDate='" + exportDate + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        InvocationPayload that = (InvocationPayload) o;
        return Objects.equals(correlationId, that.correlationId) && Objects
                .equals(analyticalDatasetPrefix, that.analyticalDatasetPrefix) && Objects
                .equals(snapshotPrefix, that.snapshotPrefix) && Objects.equals(snapshotType, that.snapshotType)
                && Objects.equals(exportDate, that.exportDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(correlationId, analyticalDatasetPrefix, snapshotPrefix, snapshotType, exportDate);
    }

    @JsonProperty("correlation_id")
    public String getCorrelationId() {
        return correlationId;
    }

    @JsonProperty("s3_prefix")
    public String getPrefix() {
        return analyticalDatasetPrefix != null && !"NOT_SET".equals(analyticalDatasetPrefix) ?
                analyticalDatasetPrefix :
                snapshotPrefix;
    }

    public String getAnalyticalDatasetPrefix() {
        return analyticalDatasetPrefix;
    }

    public String getSnapshotPrefix() {
        return snapshotPrefix;
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
