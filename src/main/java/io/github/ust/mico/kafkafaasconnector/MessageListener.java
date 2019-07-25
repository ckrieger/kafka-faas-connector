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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.cloudevents.json.Json;
import io.github.ust.mico.kafkafaasconnector.configuration.KafkaConfig;
import io.github.ust.mico.kafkafaasconnector.configuration.OpenFaaSConfig;
import io.github.ust.mico.kafkafaasconnector.exception.BatchMicoCloudEventException;
import io.github.ust.mico.kafkafaasconnector.exception.MicoCloudEventException;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import io.github.ust.mico.kafkafaasconnector.kafka.RouteHistory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Slf4j
@Component
public class MessageListener {

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private OpenFaaSConfig openFaaSConfig;

    @Autowired
    private KafkaMessageSender kafkaMessageSender;

    @Autowired
    private CloudEventManipulator cloudEventManipulator;

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
            if (!cloudEvent.getData().isPresent()) {
                // data is entirely optional
                log.debug("Received message does not include any data!");
            }
            if (cloudEvent.getExpiryDate().map(exp -> exp.compareTo(ZonedDateTime.now()) < 0).orElse(false)) {
                log.debug("Received expired message!");
                throw new MicoCloudEventException("CloudEvent has already expired!", cloudEvent);
            } else if (this.openFaaSConfig.isSkipFunctionCall()) {
                // when skipping the openFaaS function just pass on the original cloudEvent
                kafkaMessageSender.safeSendCloudEvent(cloudEvent, originalMessageId);
            } else {
                String functionResult = callFaasFunction(cloudEvent);
                ArrayList<MicoCloudEventImpl<JsonNode>> events = parseFunctionResult(functionResult, cloudEvent);
                for (MicoCloudEventImpl<JsonNode> event : events) {
                    // individually wrap each cloud event in the results for sending
                    kafkaMessageSender.safeSendCloudEvent(event, originalMessageId);
                }
            }
        } catch (MicoCloudEventException e) {
            kafkaMessageSender.safeSendErrorMessage(e.getErrorEvent(), this.kafkaConfig.getInvalidMessageTopic(), originalMessageId);
        }
    }

    /**
     * Synchronously call the configured openFaaS function.
     *
     * @param cloudEvent the cloud event used as parameter for the function
     * @return the result of the function call (in serialized form)
     */
    public String callFaasFunction(MicoCloudEventImpl<JsonNode> cloudEvent) throws MicoCloudEventException {
        try {
            URL functionUrl = openFaaSConfig.getFunctionUrl();
            log.debug("Start request to function '{}'", functionUrl.toString());
            String cloudEventSerialized = Json.encode(cloudEventManipulator.updateRouteHistoryWithFunctionCall(cloudEvent, openFaaSConfig.getFunctionName()));
            log.debug("Serialized cloud event: {}", cloudEventSerialized);
            String result = restTemplate.postForObject(functionUrl.toString(), cloudEventSerialized, String.class);
            log.debug("Faas call resulted in: '{}'", result);
            return result;
        } catch (MalformedURLException e) {
            throw new MicoCloudEventException("Failed to call faas-function. Caused by: " + e.getMessage(), cloudEvent);
        } catch (IllegalStateException e) {
            log.error("Failed to serialize CloudEvent '{}'.", cloudEvent);
            throw new MicoCloudEventException("Failed to serialize CloudEvent while calling the faas-function.", cloudEvent);
        } catch (HttpStatusCodeException e) {
            log.error("A client error occurred with http status:{} . These exceptions are triggered if the  FaaS function does not return 200 OK as the status code", e.getStatusCode(), e);
            throw new MicoCloudEventException(e.toString(), cloudEvent);
        }
    }

    /**
     * Parse the result of a faas function call.
     *
     * @param sourceCloudEvent only used for better error messages
     * @return an ArrayList of cloud events
     */
    public ArrayList<MicoCloudEventImpl<JsonNode>> parseFunctionResult(String functionResult, MicoCloudEventImpl<JsonNode> sourceCloudEvent) throws MicoCloudEventException {
        try {
            return Json.decodeValue(functionResult, new TypeReference<ArrayList<MicoCloudEventImpl<JsonNode>>>() {
            });
        } catch (IllegalStateException e) {
            log.error("Failed to parse JSON from response '{}'.", functionResult);
            throw new MicoCloudEventException("Failed to parse JSON from response from the faas-function.", sourceCloudEvent);
        }
    }

}
