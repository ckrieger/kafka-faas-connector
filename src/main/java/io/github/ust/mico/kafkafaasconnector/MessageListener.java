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
import io.github.ust.mico.kafkafaasconnector.messageprocessing.FaasController;
import io.github.ust.mico.kafkafaasconnector.messageprocessing.KafkaMessageSender;
import io.github.ust.mico.kafkafaasconnector.configuration.KafkaConfig;
import io.github.ust.mico.kafkafaasconnector.exception.MicoCloudEventException;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.util.List;

@Slf4j
@Component
public class MessageListener {

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private KafkaMessageSender kafkaMessageSender;

    @Autowired
    private FaasController faasController;

    /**
     * Entry point for incoming messages from kafka.
     *
     * @param cloudEvent the received cloud event
     */
    @KafkaListener(topics = "${kafka.input-topic}", groupId = "${kafka.group-id}")
    public void receive(MicoCloudEventImpl<JsonNode> cloudEvent) {
        log.debug("Received CloudEvent message: {}", cloudEvent);

        // Save the message Id because some faas functions create need messages with different ids.
        String originalMessageId = cloudEvent.getId();

        try {
            handleExpiredMessage(cloudEvent);

            List<MicoCloudEventImpl<JsonNode>> events = faasController.callFaasFunction(cloudEvent);
            events.forEach(event -> kafkaMessageSender.safeSendCloudEvent(event, originalMessageId));

        } catch (MicoCloudEventException e) {
            kafkaMessageSender.safeSendErrorMessage(e.getErrorEvent(), this.kafkaConfig.getInvalidMessageTopic(), originalMessageId);
        }
    }

    /**
     * Logs expired messages and throw a {@code MicoCloudEventException} if the message is expired.
     *
     * @param cloudEvent
     * @throws MicoCloudEventException
     */
    private void handleExpiredMessage(MicoCloudEventImpl<JsonNode> cloudEvent) throws MicoCloudEventException {
        if (isMessageExpired(cloudEvent)) {
            log.debug("Received expired message!");
            throw new MicoCloudEventException("CloudEvent has already expired!", cloudEvent);
        }
    }

    /**
     * Checks if the message is expired
     *
     * @param cloudEvent
     * @return
     */
    private boolean isMessageExpired(MicoCloudEventImpl<JsonNode> cloudEvent) {
        return cloudEvent.getExpiryDate().map(exp -> exp.compareTo(ZonedDateTime.now()) < 0).orElse(false);
    }

}
