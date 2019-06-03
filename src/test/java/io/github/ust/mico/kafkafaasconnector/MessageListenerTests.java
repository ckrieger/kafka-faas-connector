package io.github.ust.mico.kafkafaasconnector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;

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

    // TODO explore why autowired fails here
    private MessageListener listener = new MessageListener();

    @BeforeClass
    public static void setup() {
        String broker = System.getProperty("spring.embedded.kafka.brokers");
        System.setProperty("spring.kafka.bootstrap-servers", broker);
        System.getProperty("kafka.bootstrap-servers", broker);
        //System.setProperty("spring.kafka.bootstrap-servers",
        //            broker.getEmbeddedKafka().getBrokersAsString());
    }

    @Test
    public void parseFunctionResult() {
        ArrayList<MicoCloudEventImpl<JsonNode>> result = this.listener.parseFunctionResult("[]", null);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

}
