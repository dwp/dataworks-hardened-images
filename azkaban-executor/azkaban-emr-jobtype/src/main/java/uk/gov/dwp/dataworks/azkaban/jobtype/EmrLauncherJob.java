package uk.gov.dwp.dataworks.azkaban.jobtype;

import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.utils.Props;
import org.apache.log4j.Logger;

public class EmrLauncherJob extends AbstractProcessJob {

    public EmrLauncherJob(String jobId, Props sysProps, Props jobProps, Logger log) {
        super(jobId, sysProps, jobProps, log);
    }

    @Override
    public void run() {

    }


}
