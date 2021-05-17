package uk.gov.dwp.dataworks.azkaban.domain;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.dwp.dataworks.azkaban.services.PipelineMetadataService.CORRELATION_ID_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.PipelineMetadataService.DATE_FIELD;

class LambdaPayloadTest {

    @Test
    public void shouldUseValuesIfKeysPresent() {
        Map<String, AttributeValue> from = new HashMap<>();
        from.put(CORRELATION_ID_FIELD, attributeValue(CORRELATION_ID_VALUE));
        from.put(SNAPSHOT_TYPE_KEY, attributeValue(SNAPSHOT_TYPE_VALUE));
        from.put(DATE_FIELD, attributeValue(DATE_VALUE));
        LambdaPayload payload = LambdaPayload.from(from);
        assertEquals(CORRELATION_ID_VALUE, payload.getCorrelationId());
        assertEquals(SNAPSHOT_TYPE_VALUE, payload.getSnapshotType());
        assertEquals(DATE_VALUE, payload.getExportDate());
    }

    @Test
    public void shouldUseDefaultsIfKeysNotPresent() {
        Map<String, AttributeValue> from = new HashMap<>();
        LambdaPayload payload = LambdaPayload.from(from);
        assertEquals(DEFAULT_VALUE, payload.getCorrelationId());
        assertEquals(DEFAULT_VALUE, payload.getSnapshotType());
        assertEquals(DEFAULT_VALUE, payload.getPrefix());
        assertEquals(DEFAULT_VALUE, payload.getExportDate());
        assertEquals(DEFAULT_VALUE, payload.getPrefix());
    }

    @Test
    public void shouldUseAnalyticalDataSetPrefixIfPresent() {
        Map<String, AttributeValue> from = new HashMap<>();
        from.put(ANALYTICAL_DATASET_PREFIX_KEY, attributeValue(ANALYTICAL_DATASET_PREFIX_VALUE));
        from.put(S3_PREFIX_KEY, attributeValue(S3_PREFIX_VALUE));
        from.put(S3_SNAPSHOTS_PREFIX_KEY, attributeValue(S3_SNAPSHOTS_PREFIX_VALUE));
        LambdaPayload payload = LambdaPayload.from(from);
        assertEquals(ANALYTICAL_DATASET_PREFIX_VALUE, payload.getPrefix());
    }

    @Test
    public void shouldUseS3PrefixIfNoAnalyticalDatasetPrefixPresent() {
        Map<String, AttributeValue> from = new HashMap<>();
        from.put(S3_PREFIX_KEY, attributeValue(S3_PREFIX_VALUE));
        from.put(S3_SNAPSHOTS_PREFIX_KEY, attributeValue(S3_SNAPSHOTS_PREFIX_VALUE));
        LambdaPayload payload = LambdaPayload.from(from);
        assertEquals(S3_PREFIX_VALUE, payload.getPrefix());
    }

    @Test
    public void shouldUseS3SnapshotPrefixIfNoOtherPrefixPresent() {
        Map<String, AttributeValue> from = new HashMap<>();
        from.put(S3_SNAPSHOTS_PREFIX_KEY, attributeValue(S3_SNAPSHOTS_PREFIX_VALUE));
        LambdaPayload payload = LambdaPayload.from(from);
        assertEquals(S3_SNAPSHOTS_PREFIX_VALUE, payload.getPrefix());
    }

    private AttributeValue attributeValue(String value) {
        return new AttributeValue().withS(value);
    }

    private final static String ANALYTICAL_DATASET_PREFIX_KEY = "S3_Prefix_Analytical_DataSet";
    private final static String S3_PREFIX_KEY = "S3_Prefix";
    private final static String S3_SNAPSHOTS_PREFIX_KEY = "S3_Prefix_Snapshots";
    private final static String SNAPSHOT_TYPE_KEY = "Snapshot_Type";

    private final static String ANALYTICAL_DATASET_PREFIX_VALUE = "S3_PREFIX_ANALYTICAL_DATA_SET";
    private final static String CORRELATION_ID_VALUE = "CORRELATION_ID";
    private final static String DATE_VALUE = "DATE";
    private final static String S3_PREFIX_VALUE = "S3_PREFIX";
    private final static String S3_SNAPSHOTS_PREFIX_VALUE = "S3_PREFIX_SNAPSHOTS";
    private final static String SNAPSHOT_TYPE_VALUE = "SNAPSHOT_TYPE";
    private final static String DEFAULT_VALUE = "NOT_SET";
}
