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

import com.fasterxml.jackson.databind.JsonNode;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import io.github.ust.mico.kafkafaasconnector.messageprocessing.KafkaMessageSender;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@EnableAutoConfiguration
@AutoConfigureMockMvc
@ActiveProfiles("testing")
public class KafkaMessageSenderTests {

    @Autowired
    KafkaMessageSender kafkaMessageSender;

    /**
     * We need the embedded Kafka to successfully create the context.
     */
    private final EmbeddedKafkaBroker embeddedKafka = broker.getEmbeddedKafka();

    // https://docs.spring.io/spring-kafka/docs/2.2.6.RELEASE/reference/html/#kafka-testing-junit4-class-rule
    @ClassRule
    public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1, false);


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

        assertTrue("The message should be filtered out", kafkaMessageSender.isTestMessageCompleted(cloudEventSimple, testFilterTopic));
    }

    /**
     * Only "FilterOutBeforeTopic = message destination" is not enough to filter a message out. It needs to be a test message.
     */
    @Test
    public void testNotFilterOutCheck() {
        MicoCloudEventImpl<JsonNode> cloudEventSimple = CloudEventTestUtils.basicCloudEventWithRandomId();
        String testFilterTopic = "TestFilterTopic";
        cloudEventSimple.setFilterOutBeforeTopic(testFilterTopic);

        assertFalse("The message not should be filtered out, because it is not a test message", kafkaMessageSender.isTestMessageCompleted(cloudEventSimple, testFilterTopic));
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

        assertFalse("The message not should be filtered out, because it has not reached the filter out topic", kafkaMessageSender.isTestMessageCompleted(cloudEventSimple, testFilterTopic + "Difference"));
    }
}
