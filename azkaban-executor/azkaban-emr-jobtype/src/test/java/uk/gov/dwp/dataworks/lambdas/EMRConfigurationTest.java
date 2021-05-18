package uk.gov.dwp.dataworks.lambdas;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class EMRConfigurationTest {

    @BeforeAll
    public static void setup() {
        System.out.println("EMRStep unit test setup.");
    }

    @Test
    public void hasCorrectJsonStructureNameOnly() throws IOException {
        EMRConfiguration test = EMRConfiguration.builder().withName("test_name").build();
        ObjectMapper mapper = new ObjectMapper();
        String event = mapper.writeValueAsString(test);

        assertEquals("{'overrides':{'Name':'test_name'}}".replaceAll("'", "\""), event);
    }

    @Test
    public void hasCorrectJsonStructureNameAndConfigLocation() throws IOException {
        EMRConfiguration test = EMRConfiguration.builder().withName("test_name").withS3Overrides("test/config_path").build();
        ObjectMapper mapper = new ObjectMapper();
        String event = mapper.writeValueAsString(test);

        JsonAssertions.assertThatJson(event)
                .isEqualTo("{\"overrides\":{\"Name\":\"test_name\"},\"s3_overrides\":{\"emr_launcher_config_s3_folder\":\"test/config_path\"}}");
    }

    @Test
    public void hasCorrectJsonStructureNameAndConfigLocationWhenNull() throws IOException {
        EMRConfiguration test = EMRConfiguration.builder().withName("test_name").withS3Overrides(null).build();
        ObjectMapper mapper = new ObjectMapper();
        String event = mapper.writeValueAsString(test);

        JsonAssertions.assertThatJson(event)
                .isEqualTo("{\"overrides\":{\"Name\":\"test_name\"}}");
    }
}
