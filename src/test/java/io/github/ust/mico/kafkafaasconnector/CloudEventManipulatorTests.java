package io.github.ust.mico.kafkafaasconnector;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import io.github.ust.mico.kafkafaasconnector.messageprocessing.CloudEventManipulator;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class CloudEventManipulatorTests {
    /**
     * Don't load the application context to speed up testing
     */


    /**
     * Tests if the createdFrom attribute is set correctly
     */
    @Test
    public void testCreatedFrom() {
        MicoCloudEventImpl<JsonNode> cloudEventSimple = CloudEventTestUtils.basicCloudEventWithRandomId();
        final String originalMessageId = "OriginalMessageId";
        CloudEventManipulator cloudEventManipulator = new CloudEventManipulator();
        cloudEventManipulator.setMissingHeaderFields(cloudEventSimple, originalMessageId);
        assertThat("If the id changes the createdFrom attribute has to be set", cloudEventSimple.getCreatedFrom().orElse(null), is(originalMessageId));
    }

    /**
     * Tests if the createdFrom attribute is omitted if it is not necessary
     */
    @Test
    public void testNotCreatedFrom() {
        MicoCloudEventImpl<JsonNode> cloudEventSimple = CloudEventTestUtils.basicCloudEventWithRandomId();
        CloudEventManipulator cloudEventManipulator = new CloudEventManipulator();
        cloudEventManipulator.setMissingHeaderFields(cloudEventSimple, cloudEventSimple.getId());
        assertThat("If the id stays the same the createdFrom attribute must be empty", cloudEventSimple.getCreatedFrom().orElse(null), is(nullValue()));
    }
}
