package uk.gov.dwp.dataworks.azkaban.jobtype;

import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.utils.Props;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import org.apache.log4j.Logger;
import uk.gov.dwp.dataworks.azkaban.services.DataPipelineMetadataService;
import uk.gov.dwp.dataworks.azkaban.utility.AwsUtility;

public class EmrLauncherJob extends AbstractProcessJob {

//    private final DataPipelineMetadataService dataPipelineMetadataService;

    public EmrLauncherJob(String jobId, Props sysProps, Props jobProps, Logger log) {
        super(jobId, sysProps, jobProps, log);
//        this.dataPipelineMetadataService = new DataPipelineMetadataService(AwsUtility.amazonDynamoDb(awsRegion()));
    }

    @Override
    public void run() {
        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion(awsRegion())
                .build();
        System.out.println("dataPipelineMetadataService");
    }

    private String awsRegion() {
        return this.getSysProps().getString("aws.region", "eu-west-2");
    }
}
