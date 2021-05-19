package uk.gov.dwp.dataworks.azkaban.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class AbstractEmrLaunchingDelegate {

    public void cancel() {
        logger.warn("Operation has been cancelled");
        proceed.set(false);
    }

    private final static Logger logger = LoggerFactory.getLogger(AbstractEmrLaunchingDelegate.class);
    final AtomicBoolean proceed = new AtomicBoolean(true);
}
