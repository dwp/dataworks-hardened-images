package uk.gov.dwp.dataworks.azkaban.services;

public interface DynamoDbService {
    String ANALYTICAL_DATASET_PREFIX_FIELD = "S3_Prefix_Analytical_DataSet";
    String CLUSTER_ID_FIELD = "Cluster_Id";
    String CORRELATION_ID_FIELD = "Correlation_Id";
    String DATA_PRODUCT_FIELD = "DataProduct";
    String DATE_FIELD = "Date";
    String SNAPSHOT_PREFIX_FIELD = "S3_Prefix_Snapshots";
    String SNAPSHOT_TYPE_FIELD = "Snapshot_Type";
    String STATUS_FIELD = "Status";
    String TIME_TO_EXIST_FIELD = "TimeToExist";
}
