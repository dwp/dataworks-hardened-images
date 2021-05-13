package uk.gov.dwp.dataworks.lambdas;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class EMRConfiguration {
    private Overrides overrides;

    private S3Overrides s3Overrides;

    public EMRConfiguration(String name, String config){
        this.overrides = new Overrides(name);
        if (config != null){
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
    public S3Overrides getS3Overrides() { return s3Overrides; }


    public static class EMRConfigurationBuilder {
        private String name;
        private String config;

        public EMRConfigurationBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public EMRConfigurationBuilder withS3Overrides(String config) {
            this.config = config;
            return this;
        }

        public EMRConfiguration build() {
            return new EMRConfiguration(this.name, this.config);
        }
    }

    public static class Overrides {

        private String name;

        public Overrides(String name) {
            this.name = name;
        }

        @JsonProperty("Name")
        public String getName() {
            return this.name;
        }
    }

    public static class S3Overrides {

        private String config;

        public S3Overrides(String config) {
            this.config = config;
        }

        @JsonProperty("emr_launcher_config_s3_folder")
        public String getConfig() {
            return this.config;
        }
    }
}
