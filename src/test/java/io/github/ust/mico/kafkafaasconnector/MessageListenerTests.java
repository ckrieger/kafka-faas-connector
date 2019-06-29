/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.ust.mico.kafkafaasconnector;

import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import io.github.ust.mico.kafkafaasconnector.configuration.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
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
import io.github.ust.mico.kafkafaasconnector.kafka.RouteHistory;

import javax.annotation.PostConstruct;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.empty;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@EnableAutoConfiguration
@AutoConfigureMockMvc
@ActiveProfiles("testing")
@DirtiesContext
@Slf4j
public class MessageListenerTests {

    @Autowired
    private KafkaConfig kafkaConfig;

    private static boolean kafkaTopicInitDone = false;

    // https://docs.spring.io/spring-kafka/docs/2.2.6.RELEASE/reference/html/#kafka-testing-junit4-class-rule
    @ClassRule
    public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1, false);

    @Autowired
    MessageListener messageListener;

    @PostConstruct
    public void before(){
        //We need to add them outside of the rule because the autowired kakfaConfig is not accessible from the static rule
        //We can not use @BeforeClass which is only executed once because it has to be static and we do not have access to the autowired kakfaConfig

        Set<String> requiredTopics = MicoKafkaTestUtils.getRequiredTopics(kafkaConfig);
        EmbeddedKafkaBroker embeddedKafka = broker.getEmbeddedKafka();
        Set<String> alreadySetTopics = MicoKafkaTestUtils.requestActuallySetTopics(embeddedKafka);
        requiredTopics.removeAll(alreadySetTopics);
        requiredTopics.forEach(topic -> embeddedKafka.addTopics(topic));
    }

    @Test
    public void parseEmptyFunctionResult() {
        ArrayList<MicoCloudEventImpl<JsonNode>> result = this.messageListener.parseFunctionResult("[]", null);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void parseFunctionResult() throws JsonProcessingException {
        MicoCloudEventImpl<JsonNode> cloudEvent1 = CloudEventTestUtils.basicCloudEvent("CloudEvent1");
        MicoCloudEventImpl<JsonNode> cloudEvent2 = CloudEventTestUtils.basicCloudEvent("CloudEvent2");
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
        Consumer<String, MicoCloudEventImpl<JsonNode>> consumer = MicoKafkaTestUtils.getKafkaConsumer(embeddedKafka);
        embeddedKafka.consumeFromAllEmbeddedTopics(consumer);

        KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> template = MicoKafkaTestUtils.getKafkaProducer(embeddedKafka);

        String eventId = "CloudEventExpired";

        // generate and send cloud event message
        MicoCloudEventImpl<JsonNode> cloudEvent = CloudEventTestUtils.setPastExpiryDate(CloudEventTestUtils.basicCloudEvent(eventId));
        template.send(kafkaConfig.getInputTopic(), "0", cloudEvent);

        // consume at least the message send by this unit test
        ConsumerRecords<String, MicoCloudEventImpl<JsonNode>> records = KafkaTestUtils.getRecords(consumer, 100);
        // second consume to catch message(s) sent by unit under test
        ConsumerRecords<String, MicoCloudEventImpl<JsonNode>> records2 = KafkaTestUtils.getRecords(consumer, 1000);

        // test for expired cloud event message on topics other then the input topic
        java.util.function.Consumer<? super ConsumerRecord<String, MicoCloudEventImpl<JsonNode>>> predicate = record -> {
            if (record.value().getId().equals(eventId)) {
                if (!record.topic().equals(kafkaConfig.getInputTopic())) {
                    assertNotEquals("The expired message was wrongly processed and sent to the output channel!", record.topic(), kafkaConfig.getOutputTopic());
                }
            }
        };
        records.forEach(predicate);
        records2.forEach(predicate);

        // Don't forget to detach the consumer from kafka!
        MicoKafkaTestUtils.unsubscribeConsumer(consumer);
    }

    /**
     * Test that the route history gets updated.
     */
    @Test
    public void testRouteHistory() {
        EmbeddedKafkaBroker embeddedKafka = broker.getEmbeddedKafka();
        Consumer<String, MicoCloudEventImpl<JsonNode>> consumer = MicoKafkaTestUtils.getKafkaConsumer(embeddedKafka);
        embeddedKafka.consumeFromAllEmbeddedTopics(consumer);

        KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> template = MicoKafkaTestUtils.getKafkaProducer(embeddedKafka);

        String eventIdSimple = "routeHistorySimple";
        String eventIdMultiStep = "routeHistoryMultiStep";
        String eventIdMultiDest = "routeHistoryMultiDest";

        // generate and send cloud event message
        MicoCloudEventImpl<JsonNode> cloudEventSimple = CloudEventTestUtils.basicCloudEvent(eventIdSimple);
        MicoCloudEventImpl<JsonNode> cloudEventMultiStep = CloudEventTestUtils.addSingleTopicRoutingStep(
            CloudEventTestUtils.addSingleTopicRoutingStep(
                CloudEventTestUtils.basicCloudEvent(eventIdMultiStep),
                TestConstants.ROUTING_TOPIC_1
            ),
            kafkaConfig.getInputTopic()
        );
        // test that messages with multiple destinations don't share the routing history (mutable list)
        ArrayList<String> destinations = new ArrayList<>();
        destinations.add(kafkaConfig.getOutputTopic());
        destinations.add(TestConstants.ROUTING_TOPIC_1);
        MicoCloudEventImpl<JsonNode> cloudEventMultiDest = CloudEventTestUtils.addMultipleTopicRoutingSteps(
            CloudEventTestUtils.basicCloudEvent(eventIdMultiDest),
            destinations
        );
        template.send(kafkaConfig.getInputTopic(), "0", cloudEventSimple);
        template.send(kafkaConfig.getInputTopic(), "0", cloudEventMultiStep);
        template.send(kafkaConfig.getInputTopic(), "0", cloudEventMultiDest);

        ArrayList<ConsumerRecord<String, MicoCloudEventImpl<JsonNode>>> events = new ArrayList<>();
        // consume all message send during this unit test
        int lastSize = events.size();
        while (events.size() == 0 || lastSize < events.size()) {
            lastSize = events.size();
            KafkaTestUtils.getRecords(consumer, 1000).forEach(events::add);
        }

        // test for expired cloud event message on topics other then the input topic
        java.util.function.Consumer<? super ConsumerRecord<String, MicoCloudEventImpl<JsonNode>>> predicate = record -> {
            if (record.value().getId().equals("eventId")) {
                if (!record.topic().equals(kafkaConfig.getInputTopic())) {
                    assertNotEquals("The expired message was wrongly processed and sent to the output channel!", record.topic(), kafkaConfig.getOutputTopic());
                }
            }
        };
        events.forEach(record -> {
            if (!record.topic().equals(kafkaConfig.getInputTopic())) {
                List<RouteHistory> history = record.value().getRoute().orElse(new ArrayList<>());
                assertTrue("Route history was not set/empty.", history.size() > 0);
                RouteHistory lastStep = history.get(history.size() - 1);
                assertEquals("Route history step has wrong type (should be 'topic')", "topic", lastStep.getType().orElse(""));
                assertEquals(lastStep.getId().orElse(""), record.topic());
                if (record.value().getId().equals(eventIdMultiStep)) {
                    // test multi step routing history
                    assertTrue("Route history is missing a step.", history.size() > 1);
                    RouteHistory firstStep = history.get(0);
                    assertEquals("Route history step has wrong type (should be 'topic')", "topic", firstStep.getType().orElse(""));
                    assertEquals(firstStep.getId().orElse(""), kafkaConfig.getInputTopic());
                }
            }
        });

        // Don't forget to detach the consumer from kafka!
        MicoKafkaTestUtils.unsubscribeConsumer(consumer);
    }

    /**
     * Tests if the need to filter out a message with "isTestMessage = true" and a "FilterOutBeforeTopic = message destination"
     * is correctly recognized.
     */
    @Test
    public void testFilterOutCheck() {
        MicoCloudEventImpl<JsonNode> cloudEventSimple = CloudEventTestUtils.basicCloudEventWithRandomId();
        String testFilterTopic = "TestFilterTopic";
        cloudEventSimple.setFilterOutBeforeTopic(testFilterTopic);
        cloudEventSimple.setIsTestMessage(true);

        assertTrue("The message should be filtered out", messageListener.isTestMessageCompleted(cloudEventSimple,testFilterTopic));
    }

    /**
     * Only "FilterOutBeforeTopic = message destination" is not enough to filter a message out. It needs to be a test message.
     */
    @Test
    public void testNotFilterOutCheck() {
        MicoCloudEventImpl<JsonNode> cloudEventSimple = CloudEventTestUtils.basicCloudEventWithRandomId();
        String testFilterTopic = "TestFilterTopic";
        cloudEventSimple.setFilterOutBeforeTopic(testFilterTopic);

        assertFalse("The message not should be filtered out, because it is not a test message", messageListener.isTestMessageCompleted(cloudEventSimple,testFilterTopic));
    }

    /**
     * Tests if a test message with "FilterOutBeforeTopic != message destination" will wrongly be filtered out.
     */
    @Test
    public void testNotFilterOutCheckDifferentTopics() {
        MicoCloudEventImpl<JsonNode> cloudEventSimple = CloudEventTestUtils.basicCloudEventWithRandomId();
        String testFilterTopic = "TestFilterTopic";
        cloudEventSimple.setFilterOutBeforeTopic(testFilterTopic);
        cloudEventSimple.setIsTestMessage(true);

        assertFalse("The message not should be filtered out, because it has not reached the filter out topic", messageListener.isTestMessageCompleted(cloudEventSimple,testFilterTopic + "Difference"));
    }

    /**
     * Test if a not test message is filtered out
     */
    @Test
    public void testNotFilterNormalMessages() {
        EmbeddedKafkaBroker embeddedKafka = broker.getEmbeddedKafka();
        Consumer<String, MicoCloudEventImpl<JsonNode>> consumer = MicoKafkaTestUtils.getKafkaConsumer(embeddedKafka);
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, kafkaConfig.getOutputTopic());
        KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> template = MicoKafkaTestUtils.getKafkaProducer(embeddedKafka);

        MicoCloudEventImpl<JsonNode> cloudEventSimple = CloudEventTestUtils.basicCloudEventWithRandomId();
        template.send(kafkaConfig.getInputTopic(), "0", cloudEventSimple);

        ConsumerRecord<String, MicoCloudEventImpl<JsonNode>> event = KafkaTestUtils.getSingleRecord(consumer, kafkaConfig.getOutputTopic(), 1000);
        assertThat(event, is(notNullValue()));
        assertThat(event.value().getId(), is(equalTo(cloudEventSimple.getId())));

        // Don't forget to detach the consumer from kafka!
        MicoKafkaTestUtils.unsubscribeConsumer(consumer);
    }

    /**
     * Tests if a test message which should be filtered out (FilterOutBeforeTopic = message destination) is correctly filtered out.
     */
    @Test
    public void testFilterTestMessages() {
        EmbeddedKafkaBroker embeddedKafka = broker.getEmbeddedKafka();
        Consumer<String, MicoCloudEventImpl<JsonNode>> consumer = MicoKafkaTestUtils.getKafkaConsumer(embeddedKafka);
        embeddedKafka.consumeFromEmbeddedTopics(consumer, kafkaConfig.getTestMessageOutputTopic(), kafkaConfig.getOutputTopic());
        KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> template = MicoKafkaTestUtils.getKafkaProducer(embeddedKafka);

        MicoCloudEventImpl<JsonNode> cloudEventSimple = CloudEventTestUtils.basicCloudEventWithRandomId();
        cloudEventSimple.setIsTestMessage(true);
        cloudEventSimple.setFilterOutBeforeTopic(kafkaConfig.getOutputTopic());
        template.send(kafkaConfig.getInputTopic(), "0", cloudEventSimple);

        ConsumerRecord<String, MicoCloudEventImpl<JsonNode>> eventFilteredTestMessageTopic = KafkaTestUtils.getSingleRecord(consumer, kafkaConfig.getTestMessageOutputTopic(), 1000);
        ArrayList<ConsumerRecord<String, MicoCloudEventImpl<JsonNode>>> otherEvents = new ArrayList<>();
        KafkaTestUtils.getRecords(consumer, 1000).forEach(otherEvents::add);
        assertThat("The event should be on the filtered test topc", eventFilteredTestMessageTopic, is(notNullValue()));
        assertThat("The ids should be the same", eventFilteredTestMessageTopic.value().getId(), is(equalTo(cloudEventSimple.getId())));
        assertThat("There should not any messages on other topics", otherEvents, is(empty()));

        // Don't forget to detach the consumer from kafka!
        MicoKafkaTestUtils.unsubscribeConsumer(consumer);
    }
}
