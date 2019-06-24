package io.github.ust.mico.kafkafaasconnector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.ZonedDateTime;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import io.cloudevents.json.Json;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@EnableAutoConfiguration
@AutoConfigureMockMvc
@ActiveProfiles("testing")
@DirtiesContext
public class MessageListenerTests {

    // https://docs.spring.io/spring-kafka/docs/2.2.6.RELEASE/reference/html/#kafka-testing-junit4-class-rule
    @ClassRule
    public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1, false,
        TestConstants.INPUT_TOPIC, TestConstants.OUTPUT_TOPIC,
        TestConstants.DEAD_LETTER_TOPIC, TestConstants.INVALID_MESSAGE_TOPIC,
        TestConstants.ROUTING_TOPIC_1, TestConstants.ROUTING_TOPIC_2,
        TestConstants.ROUTING_TOPIC_3, TestConstants.ROUTING_TOPIC_4);

    @Autowired
    private MessageListener messageListener;

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

    /**
     * Test that expired cloud events actually are ignored.
     */
    @Test
    public void testExpiredMessage() {
        EmbeddedKafkaBroker embeddedKafka = broker.getEmbeddedKafka();
        Consumer<String, MicoCloudEventImpl<JsonNode>> consumer = TestConstants.getKafkaConsumer(embeddedKafka);
        embeddedKafka.consumeFromAllEmbeddedTopics(consumer);

        KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> template = TestConstants.getKafkaProducer(embeddedKafka);

        String eventId = "CloudEventExpired";

        // generate and send cloud event message
        MicoCloudEventImpl<JsonNode> cloudEvent = TestConstants.setPastExpiryDate(TestConstants.basicCloudEvent(eventId));
        template.send(TestConstants.INPUT_TOPIC, "0", cloudEvent);

        // consume at least the message send by this unit test
        ConsumerRecords<String, MicoCloudEventImpl<JsonNode>> records = KafkaTestUtils.getRecords(consumer, 100);
        // second consume to catch message(s) sent by unit under test
        ConsumerRecords<String, MicoCloudEventImpl<JsonNode>> records2 = KafkaTestUtils.getRecords(consumer, 1000);

        // test for expired cloud event message on topics other then the input topic
        java.util.function.Consumer<? super ConsumerRecord<String, MicoCloudEventImpl<JsonNode>>> predicate = record -> {
            if (record.value().getId().equals(eventId)) {
                if (!record.topic().equals(TestConstants.INPUT_TOPIC)) {
                    assertNotEquals("The expired message was wrongly processed and sent to the output channel!", record.topic(), TestConstants.OUTPUT_TOPIC);
                }
            }
        };
        records.forEach(predicate);
        records2.forEach(predicate);
    }

}
