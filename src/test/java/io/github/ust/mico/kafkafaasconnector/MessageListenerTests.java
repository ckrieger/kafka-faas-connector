package io.github.ust.mico.kafkafaasconnector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@EnableAutoConfiguration
@AutoConfigureMockMvc
@ActiveProfiles("dev")
public class MessageListenerTests {

    // TODO explore embedded broker for running tests
    // https://docs.spring.io/spring-kafka/docs/2.2.6.RELEASE/reference/html/#kafka-testing-junit4-class-rule
    @ClassRule
    public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1, false, "${kafka.input-topic}");

    // TODO explore why autowired fails here
    private MessageListener listener = new MessageListener();

    @BeforeClass
    public static void setup() {
        System.setProperty("spring.kafka.bootstrap-servers",
                    broker.getEmbeddedKafka().getBrokersAsString());
    }


    @Test
    public void parseFunctionResult() {
        ArrayList<MicoCloudEventImpl<JsonNode>> result = this.listener.parseFunctionResult("[]", null);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

}
