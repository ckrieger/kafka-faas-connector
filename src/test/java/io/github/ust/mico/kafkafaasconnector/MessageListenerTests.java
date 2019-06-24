package io.github.ust.mico.kafkafaasconnector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import io.cloudevents.json.Json;
import io.github.ust.mico.kafkafaasconnector.TestConstants;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@EnableAutoConfiguration
@AutoConfigureMockMvc
@ActiveProfiles("testing")
@DirtiesContext
public class MessageListenerTests {

    // TODO explore embedded broker for running tests
    // https://docs.spring.io/spring-kafka/docs/2.2.6.RELEASE/reference/html/#kafka-testing-junit4-class-rule
    @ClassRule
    public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1, false,
        TestConstants.INPUT_TOPIC, TestConstants.OUTPUT_TOPIC,
        TestConstants.DEAD_LETTER_TOPIC, TestConstants.INVALID_MESSAGE_TOPIC,
        TestConstants.ROUTING_TOPIC_1, TestConstants.ROUTING_TOPIC_2,
        TestConstants.ROUTING_TOPIC_3, TestConstants.ROUTING_TOPIC_4);

    @Autowired
    private MessageListener messageListener;

    @BeforeClass
    public static void setup() {
        String broker = System.getProperty("spring.embedded.kafka.brokers");
        System.setProperty("spring.kafka.bootstrap-servers", broker);
        System.getProperty("kafka.bootstrap-servers", broker);
        //System.setProperty("spring.kafka.bootstrap-servers",
        //            broker.getEmbeddedKafka().getBrokersAsString());
    }

    @Test
    public void parseEmptyFunctionResult() {
        ArrayList<MicoCloudEventImpl<JsonNode>> result = this.messageListener.parseFunctionResult("[]", null);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void parseFunctionResult() throws JsonProcessingException {
        MicoCloudEventImpl<JsonNode> cloudEvent1 = TestConstants.basicCloudEvent("CloudEvent1");
        MicoCloudEventImpl<JsonNode> cloudEvent2 = TestConstants.basicCloudEvent("CloudEvent2");
        ArrayList<MicoCloudEventImpl<JsonNode>> input = new ArrayList<>();
        input.add(cloudEvent1);
        input.add(cloudEvent2);
        String functionInput = Json.encode(input);
        ArrayList<MicoCloudEventImpl<JsonNode>> result = this.messageListener.parseFunctionResult(functionInput, null);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(result.get(0).getId(), cloudEvent1.getId());
        assertEquals(result.get(0).getSource(), cloudEvent1.getSource());
        assertEquals(result.get(0).getType(), cloudEvent1.getType());
        assertTrue(result.get(0).getTime().get().isEqual(cloudEvent1.getTime().get()));
        assertEquals(result.get(1).getId(), cloudEvent2.getId());
        assertEquals(result.get(0).getRoutingSlip(), cloudEvent2.getRoutingSlip());
    }

}
