package uk.gov.dwp.dataworks.azkaban.utility;

import azkaban.Constants;
import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;
import org.apache.log4j.Logger;

import java.util.function.Function;

import static uk.gov.dwp.dataworks.azkaban.jobtype.EMRStep.AZKABAN_SERVICE_USER;
import static uk.gov.dwp.dataworks.azkaban.jobtype.EMRStep.USE_EMR_USER;

public class UserUtility {

    public static String effectiveUser(Props jobProps, Props systemProps) {

        String serviceUser = systemProps.getString(AZKABAN_SERVICE_USER, "");
        if (!serviceUser.trim().equals("")) {
            return serviceUser;
        }

        String submitUser = jobProps.getString(CommonJobProperties.SUBMIT_USER, "");
        String proxyUser = jobProps.getString(Constants.JobProperties.USER_TO_PROXY, "");

        if (!proxyUser.trim().equals("")) {
            if (systemProps.getBoolean(USE_EMR_USER, false)) {
                if (!submitUser.equals(proxyUser)) {
                    throw new RuntimeException(
                            "'" + Constants.JobProperties.USER_TO_PROXY + "' property (" + proxyUser + ") "
                                    + "must be the same as '" + CommonJobProperties.SUBMIT_USER + "' property ("
                                    + submitUser + ") " + "when '" + USE_EMR_USER + "' (" + systemProps
                                    .getBoolean(USE_EMR_USER, false) + ") is true.");
                }
            }
            return proxyUser;
        }

        if (submitUser.trim().equals("")) {
            throw new RuntimeException("No service user, submit user or proxy user specified.");
        }

        return submitUser;
    }

    public static String emrUser(Props systemProps, String effectiveUser, Function<String, String> groupFunction) {
        String serviceUser = systemProps.getString(AZKABAN_SERVICE_USER, "");
        boolean disallowProxyUser = systemProps.getBoolean(USE_EMR_USER, false);
        logger.info("Got service user from properties: '" + serviceUser + "'.");
        logger.info("Disallowing proxy users: '" + disallowProxyUser + "'.");
        return !serviceUser.trim().equals("") ?
                serviceUser.trim() :
                (disallowProxyUser ? effectiveUser : groupFunction.apply(effectiveUser));
    }

    private final static Logger logger = Logger.getLogger(UserUtility.class);
}
