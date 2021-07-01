package uk.gov.dwp.dataworks.lambdas;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class EMRConfiguration {
    private final Overrides overrides;
    private final Boolean copySecurityConfiguration;
    private S3Overrides s3Overrides;

    public EMRConfiguration(String name, String config, boolean copySecurityConfiguration) {
        this.overrides = new Overrides(name);

        /* Note we only set this property for true values so as to be backwards compatible with older
           versions of the EMR launcher API. Once we are happy all environments and consumers are using
           this property, we can make the value explicit
         */
        this.copySecurityConfiguration = copySecurityConfiguration ? Boolean.TRUE : null;
        if (config != null) {
            this.s3Overrides = new S3Overrides(config);
        }
    }

    public static EMRConfigurationBuilder builder() {
        return new EMRConfigurationBuilder();
    }

    public Overrides getOverrides() {
        return overrides;
    }

    @JsonProperty("s3_overrides")
    public S3Overrides getS3Overrides() {
        return s3Overrides;
    }

    @JsonProperty("copy_secconfig")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Boolean getCopySecurityConfiguration() { return copySecurityConfiguration; }

    public static class EMRConfigurationBuilder {
        private String name;
        private String config;
        private boolean copySecurityConfiguration;

        public EMRConfigurationBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public EMRConfigurationBuilder withS3Overrides(String config) {
            this.config = config;
            return this;
        }

        public EMRConfiguration build() {
            return new EMRConfiguration(this.name, this.config, this.copySecurityConfiguration);
        }

        public EMRConfigurationBuilder withCopySecurityConfiguration(boolean copyConfig) {
            this.copySecurityConfiguration = copyConfig;
            return this;
        }
    }

    public static class Overrides {

        private final String name;

        public Overrides(String name) {
            this.name = name;
        }

        @JsonProperty("Name")
        public String getName() {
            return this.name;
        }
    }

    public static class S3Overrides {

        private final String config;

        public S3Overrides(String config) {
            this.config = config;
        }

        @JsonProperty("emr_launcher_config_s3_folder")
        public String getConfig() {
            return this.config;
        }
    }
}
