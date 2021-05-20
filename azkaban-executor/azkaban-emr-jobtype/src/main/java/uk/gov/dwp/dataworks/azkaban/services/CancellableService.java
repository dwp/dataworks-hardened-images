package uk.gov.dwp.dataworks.azkaban.services;


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class CancellableService {

    public void cancel() {
        logger.warn("Operation has been cancelled");
        proceed.set(false);
    }

    private final static Logger logger = LogManager.getLogger(CancellableService.class);
    final AtomicBoolean proceed = new AtomicBoolean(true);
}
