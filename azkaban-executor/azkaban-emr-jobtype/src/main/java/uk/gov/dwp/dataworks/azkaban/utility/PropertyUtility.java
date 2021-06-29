package uk.gov.dwp.dataworks.azkaban.utility;

import azkaban.utils.Props;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class PropertyUtility {

    public final static String COLLECTION_DEPENDENCIES_PARAMETER_NAME = "collection.dependencies";
    public final static String EXPORT_DATE_PARAMETER_NAME = "export.date";
    public static final String SKIP_NOTIFICATIONS_PARAMETER_NAME = "skip.notifications";
    private final Props jobProperties;

    public PropertyUtility(Props jobProperties) {
        this.jobProperties = jobProperties;
    }

    public List<String> collectionDependencies() {
        return Arrays.stream(jobProperties.getString(COLLECTION_DEPENDENCIES_PARAMETER_NAME, "").split("\\s*,\\s*"))
                     .filter(s -> s.length() > 0).collect(Collectors.toList());
    }

    public boolean skipNotifications() {
        return jobProperties.getString(SKIP_NOTIFICATIONS_PARAMETER_NAME, "false").equalsIgnoreCase("true");
    }

    public String exportDate() {
        return jobProperties.getString(EXPORT_DATE_PARAMETER_NAME, new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
    }

}
