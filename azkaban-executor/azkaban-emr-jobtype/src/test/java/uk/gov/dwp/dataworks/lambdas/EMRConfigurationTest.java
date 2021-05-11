package uk.gov.dwp.dataworks.lambdas;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.JsonNode;

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
        EMRConfiguration test = EMRConfiguration.builder().withName("test_name").withS3Overrides("test/config.yml").build();
        ObjectMapper mapper = new ObjectMapper();
        String event = mapper.writeValueAsString(test);
        try {

            JsonNode jsonNode = mapper.readValue(event, JsonNode.class);
            assertEquals("{'Name':'test_name'}", jsonNode.get("overrides").asText());
            assertEquals("{'emr_launcher_config_s3_folder':'test/config.yml'}", jsonNode.get("s3_overrides").asText());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
