package uk.gov.dwp.dataworks.azkaban.model;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.dwp.dataworks.azkaban.services.DependencyService.CORRELATION_ID_FIELD;
import static uk.gov.dwp.dataworks.azkaban.services.DependencyService.DATE_FIELD;

class InvocationPayloadTest {

    @Test
    public void shouldUseValuesIfKeysPresent() {
        Map<String, AttributeValue> from = new HashMap<>();
        from.put(CORRELATION_ID_FIELD, attributeValue(CORRELATION_ID_VALUE));
        from.put(SNAPSHOT_TYPE_KEY, attributeValue(SNAPSHOT_TYPE_VALUE));
        from.put(DATE_FIELD, attributeValue(DATE_VALUE));
        InvocationPayload payload = InvocationPayload.from(from);
        assertEquals(CORRELATION_ID_VALUE, payload.getCorrelationId());
        assertEquals(SNAPSHOT_TYPE_VALUE, payload.getSnapshotType());
        assertEquals(DATE_VALUE, payload.getExportDate());
    }

    @Test
    public void shouldUseDefaultsIfKeysNotPresent() {
        Map<String, AttributeValue> from = new HashMap<>();
        InvocationPayload payload = InvocationPayload.from(from);
        assertEquals(DEFAULT_VALUE, payload.getCorrelationId());
        assertEquals(DEFAULT_VALUE, payload.getSnapshotType());
        assertEquals(DEFAULT_VALUE, payload.getAnalyticalDatasetPrefix());
        assertEquals(DEFAULT_VALUE, payload.getExportDate());
        assertEquals(DEFAULT_VALUE, payload.getAnalyticalDatasetPrefix());
    }

    @Test
    public void shouldRenameKeysWhenSerialized() throws IOException {
        Map<String, AttributeValue> from = new HashMap<>();
        String payload = new ObjectMapper().writeValueAsString(InvocationPayload.from(from));
        Map<String, String> payloadMap = new ObjectMapper().readValue(payload, Map.class);
        assertTrue(payloadMap.containsKey("correlation_id"));
        assertTrue(payloadMap.containsKey("export_date"));
        assertTrue(payloadMap.containsKey("snapshot_type"));
        assertTrue(payloadMap.containsKey("s3_prefix"));
    }

    @Test
    public void shouldUseAnalyticalDataSetPrefixIfPresent() {
        Map<String, AttributeValue> from = new HashMap<>();
        from.put(ANALYTICAL_DATASET_PREFIX_KEY, attributeValue(ANALYTICAL_DATASET_PREFIX_VALUE));
        from.put(S3_SNAPSHOTS_PREFIX_KEY, attributeValue(S3_SNAPSHOTS_PREFIX_VALUE));
        InvocationPayload payload = InvocationPayload.from(from);
        assertEquals(ANALYTICAL_DATASET_PREFIX_VALUE, payload.getPrefix());
    }

    @Test
    public void shouldUseS3SnapshotPrefixIfNoOtherPrefixPresent() {
        Map<String, AttributeValue> from = new HashMap<>();
        from.put(S3_SNAPSHOTS_PREFIX_KEY, attributeValue(S3_SNAPSHOTS_PREFIX_VALUE));
        InvocationPayload payload = InvocationPayload.from(from);
        assertEquals(S3_SNAPSHOTS_PREFIX_VALUE, payload.getPrefix());
    }

    private AttributeValue attributeValue(String value) {
        return new AttributeValue().withS(value);
    }

    private final static String ANALYTICAL_DATASET_PREFIX_KEY = "S3_Prefix_Analytical_DataSet";
    private final static String S3_SNAPSHOTS_PREFIX_KEY = "S3_Prefix_Snapshots";
    private final static String SNAPSHOT_TYPE_KEY = "Snapshot_Type";

    private final static String ANALYTICAL_DATASET_PREFIX_VALUE = "S3_PREFIX_ANALYTICAL_DATA_SET";
    private final static String CORRELATION_ID_VALUE = "CORRELATION_ID";
    private final static String DATE_VALUE = "DATE";
    private final static String S3_SNAPSHOTS_PREFIX_VALUE = "S3_PREFIX_SNAPSHOTS";
    private final static String SNAPSHOT_TYPE_VALUE = "SNAPSHOT_TYPE";
    private final static String DEFAULT_VALUE = "NOT_SET";
}
