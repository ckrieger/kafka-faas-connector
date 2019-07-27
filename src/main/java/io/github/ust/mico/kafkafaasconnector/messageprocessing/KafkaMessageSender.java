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

package io.github.ust.mico.kafkafaasconnector.messageprocessing;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.ust.mico.kafkafaasconnector.configuration.KafkaConfig;
import io.github.ust.mico.kafkafaasconnector.exception.BatchMicoCloudEventException;
import io.github.ust.mico.kafkafaasconnector.exception.MicoCloudEventException;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Slf4j
@Service
public class KafkaMessageSender {

    @Autowired
    private KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> kafkaTemplate;

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private CloudEventManipulator cloudEventManipulator;

    /**
     * Send a cloud event using the sendCloudEvent method.
     * <p>
     * This method is safe in the sense that it does not throw exceptions and catches all exceptions during sending.
     *
     * @param cloudEvent        the cloud event to send
     * @param originalMessageId the id of the original message
     */
    public void safeSendCloudEvent(MicoCloudEventImpl<JsonNode> cloudEvent, String originalMessageId) {
        try {
            this.sendCloudEvent(cloudEvent, originalMessageId);
        } catch (MicoCloudEventException e) {
            this.safeSendErrorMessage(e.getErrorEvent(), this.kafkaConfig.getInvalidMessageTopic(), originalMessageId);
        } catch (BatchMicoCloudEventException e) {
            for (MicoCloudEventException error : e.exceptions) {
                this.safeSendErrorMessage(error.getErrorEvent(), this.kafkaConfig.getInvalidMessageTopic(), originalMessageId);
            }
        } catch (Exception e) {
            MicoCloudEventException error = new MicoCloudEventException("An error occurred while sending the cloud event.", cloudEvent);
            this.safeSendErrorMessage(error.getErrorEvent(), this.kafkaConfig.getInvalidMessageTopic(), originalMessageId);
        }
    }


    /**
     * Send a cloud event error message using the sendCloudEvent method.
     * <p>
     * This method is safe in the sense that it does not throw exceptions and catches all exceptions during sending.
     *
     * @param cloudEvent        the cloud event to send
     * @param topic             the kafka topic to send the cloud event to
     * @param originalMessageId the id of the original message
     */
    public void safeSendErrorMessage(MicoCloudEventImpl<JsonNode> cloudEvent, String topic, String originalMessageId) {
        try {
            this.sendCloudEvent(cloudEvent, topic, originalMessageId);
        } catch (Exception e) {
            log.error("Failed to process error message. Caused by: {}", e.getMessage());
        }
    }

    /**
     * Send cloud event to the specified topic.
     * <p>
     * This method also updates the route history of the cloud event before sending.
     *
     * @param cloudEvent        the cloud event to send
     * @param topic             the kafka topic to send the cloud event to
     * @param originalMessageId the id of the original message
     */
    private void sendCloudEvent(MicoCloudEventImpl<JsonNode> cloudEvent, String topic, String originalMessageId) throws MicoCloudEventException {
        try {
            cloudEvent = cloudEventManipulator.updateRouteHistoryWithTopic(cloudEvent, topic);
            // TODO commit logic/transactions
            cloudEventManipulator.setMissingHeaderFields(cloudEvent, originalMessageId);
            if (!isTestMessageCompleted(cloudEvent, topic)) {
                log.debug("Is not necessary to filter the message. Is test message '{}', filterOutBeforeTopic: '{}', targetTopic: '{}'", cloudEvent.isTestMessage(), cloudEvent.getFilterOutBeforeTopic(), topic);
                kafkaTemplate.send(topic, cloudEvent);
            } else {
                log.info("Filter out test message: '{}' to topic: '{}'", cloudEvent, kafkaConfig.getTestMessageOutputTopic());
                kafkaTemplate.send(kafkaConfig.getTestMessageOutputTopic(), cloudEvent);
            }
        } catch (Exception e) {
            throw new MicoCloudEventException("An error occurred while sending the cloud event.", e, cloudEvent);
        }
    }

    /**
     * This method checks if it is necessary to filter it out. This only works
     * for testMessages. It returns {@code true} if {@code isTestMessage} is
     * true and the param {@code topic} equals {@code filterOutBeforeTopic}.
     *
     * @param topic The target topic or destination of this message.
     * @return isTestMessage && topic.equals(filterOutBeforeTopic)
     */
    public boolean isTestMessageCompleted(MicoCloudEventImpl<JsonNode> cloudEvent, String topic) {
        return cloudEvent.isTestMessage().orElse(false) && topic.equals(cloudEvent.getFilterOutBeforeTopic().orElse(null));
    }

    /**
     * Send cloud event to default topic or topic(s) next in the routingSlip.
     *
     * @param cloudEvent        the cloud event to send
     * @param originalMessageId the id of the original message
     */
    private void sendCloudEvent(MicoCloudEventImpl<JsonNode> cloudEvent, String originalMessageId) throws MicoCloudEventException, BatchMicoCloudEventException {
        LinkedList<List<String>> routingSlip = cloudEvent.getRoutingSlip().orElse(new LinkedList<>());
        if (!routingSlip.isEmpty()) {
            List<String> destinations = routingSlip.removeLast();
            ArrayList<MicoCloudEventException> exceptions = new ArrayList<>(destinations.size());
            for (String topic : destinations) {
                try {
                    this.sendCloudEvent(cloudEvent, topic, originalMessageId);
                } catch (MicoCloudEventException e) {
                    // defer exception handling
                    exceptions.add(e);
                }
            }
            if (exceptions.size() > 0) {
                throw new BatchMicoCloudEventException((MicoCloudEventException[]) exceptions.toArray());
            }
        } else {
            // default case:
            this.sendCloudEvent(cloudEvent, this.kafkaConfig.getOutputTopic(), originalMessageId);
        }
    }

}
