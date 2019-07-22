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
import java.util.List;

@Slf4j
@Component
public class MessageListener {

    protected static final String CONTENT_TYPE = "application/json";

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> kafkaTemplate;

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private OpenFaaSConfig openFaaSConfig;

    /**
     * Entry point for incoming messages from kafka.
     *
     * @param cloudEvent
     */
    @KafkaListener(topics = "${kafka.input-topic}", groupId = "${kafka.group-id}")
    public void receive(MicoCloudEventImpl<JsonNode> cloudEvent) {
        log.debug("Received CloudEvent message: {}", cloudEvent);

        //Save the message Id because some faas functions create need messages with different ids.
        String originalMessageId = cloudEvent.getId();

        try {
            if (!cloudEvent.getData().isPresent()) {
                // data is entirely optional
                log.debug("Received message does not include any data!");
            }
            if (cloudEvent.getExpiryDate().map(exp -> exp.compareTo(ZonedDateTime.now()) < 0).orElse(false)) {
                log.debug("Received expired message!");
                throw new MicoCloudEventException("CloudEvent was expired already!", cloudEvent);
            } else if (this.openFaaSConfig.isSkipFunctionCall()) {
                // when skipping the openFaaS function just pass on the original cloudEvent
                this.safeSendCloudEvent(cloudEvent, originalMessageId);
            } else {
                String functionResult = callFaasFunction(cloudEvent);
                ArrayList<MicoCloudEventImpl<JsonNode>> events = parseFunctionResult(functionResult, cloudEvent);
                for (MicoCloudEventImpl<JsonNode> event : events) {
                    // individually wrap each cloud event in the results for sending
                    this.safeSendCloudEvent(event, originalMessageId);
                }
            }
        } catch (MicoCloudEventException e) {
            this.safeSendErrorMessage(e.getErrorEvent(), this.kafkaConfig.getInvalidMessageTopic(), originalMessageId);
        }
    }

    /**
     * Send a cloud event using the sendCloudEvent method.
     * <p>
     * This method is safe in the sence that it does not throw exceptions and catches all exceptions during sending.
     *
     * @param cloudEvent        the cloud event to send
     * @param originalMessageId the id of the original message
     */
    private void safeSendCloudEvent(MicoCloudEventImpl<JsonNode> cloudEvent, String originalMessageId) {
        try {
            this.sendCloudEvent(cloudEvent, originalMessageId);
        } catch (MicoCloudEventException e) {
            this.safeSendErrorMessage(e.getErrorEvent(), this.kafkaConfig.getInvalidMessageTopic(), originalMessageId);
        } catch (BatchMicoCloudEventException e) {
            for (MicoCloudEventException error : e.exceptions) {
                this.safeSendErrorMessage(error.getErrorEvent(), this.kafkaConfig.getInvalidMessageTopic(), originalMessageId);
            }
        } catch (Exception e) {
            MicoCloudEventException error = new MicoCloudEventException("An error occured while sending the cloud event.", cloudEvent);
            this.safeSendErrorMessage(error.getErrorEvent(), this.kafkaConfig.getInvalidMessageTopic(), originalMessageId);
        }
    }


    /**
     * Send a cloud event error message using the sendCloudEvent method.
     * <p>
     * This method is safe in the sence that it does not throw exceptions and catches all exceptions during sending.
     *
     * @param cloudEvent        the cloud event to send
     * @param topic             the kafka topic to send the cloud event to
     * @param originalMessageId the id of the original message
     */
    private void safeSendErrorMessage(MicoCloudEventImpl<JsonNode> cloudEvent, String topic, String originalMessageId) {
        try {
            this.sendCloudEvent(cloudEvent, this.kafkaConfig.getInvalidMessageTopic(), originalMessageId);
        } catch (Exception e) {
            log.error("Failed to process error message.", e);
        }
    }

    /**
     * Add a function call routing step to the routing history of the cloud event.
     *
     * @param cloudEvent the cloud event to update
     * @param functionId the id of the function applied to the cloud event next
     * @return the updated cloud event
     */
    public MicoCloudEventImpl<JsonNode> updateRouteHistoryWithFunctionCall(MicoCloudEventImpl<JsonNode> cloudEvent, String functionId) {
        return this.updateRouteHistory(cloudEvent, functionId, "faas-function");
    }

    /**
     * Add a topic routing step to the routing history of the cloud event.
     *
     * @param cloudEvent the cloud event to update
     * @param topic      the next topic the event will be sent to
     * @return the updated cloud event
     */
    public MicoCloudEventImpl<JsonNode> updateRouteHistoryWithTopic(MicoCloudEventImpl<JsonNode> cloudEvent, String topic) {
        return this.updateRouteHistory(cloudEvent, topic, "topic");
    }

    /**
     * Update the routing history in the `route` header field of the cloud event.
     *
     * @param cloudEvent the cloud event to update
     * @param id         the string id of the next routing step the message will take
     * @param type       the type of the routing step ("topic" or "faas-function")
     * @return the updated cloud event
     */
    public MicoCloudEventImpl<JsonNode> updateRouteHistory(MicoCloudEventImpl<JsonNode> cloudEvent, String id, String type) {
        RouteHistory routingStep = new RouteHistory(type, id, ZonedDateTime.now());
        List<RouteHistory> history = cloudEvent.getRoute().map(route -> new ArrayList<>(route)).orElse(new ArrayList<>());
        history.add(routingStep);
        return new MicoCloudEventImpl<JsonNode>(cloudEvent).setRoute(history);
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
            String cloudEventSerialized = Json.encode(this.updateRouteHistoryWithFunctionCall(cloudEvent, openFaaSConfig.getFunctionName()));
            log.debug("Serialized cloud event: {}", cloudEventSerialized);
            String result = restTemplate.postForObject(functionUrl.toString(), cloudEventSerialized, String.class);
            log.debug("Faas call resulted in: '{}'", result);
            return result;
        } catch (MalformedURLException e) {
            // TODO decide error behaviour and commit behaviour
        } catch (IllegalStateException e) {
            log.error("Failed to serialize CloudEvent '{}'.", cloudEvent);
            throw new MicoCloudEventException("Failed to serialize CloudEvent while calling the faas-function.", cloudEvent);
        } catch (HttpStatusCodeException e) {
            log.error("A client error occurred with http status:{} . These exceptions are triggered if the  FaaS function does not return 200 OK as the status code", e.getStatusCode(), e);
            throw new MicoCloudEventException(e.toString(), cloudEvent);
        }
        return null;
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

    /**
     * Send cloud event to default topic or topic(s) next in the routingSlip.
     *
     * @param cloudEvent        the cloud event to send
     * @param originalMessageId the id of the original message
     */
    public void sendCloudEvent(MicoCloudEventImpl<JsonNode> cloudEvent, String originalMessageId) throws MicoCloudEventException, BatchMicoCloudEventException {
        List<List<String>> routingSlip = cloudEvent.getRoutingSlip().orElse(new ArrayList<>());
        if (routingSlip.size() > 0) {
            List<String> destinations = routingSlip.get(routingSlip.size() - 1);
            routingSlip.remove(routingSlip.size() - 1);
            // Check if valid topic?
            ArrayList<MicoCloudEventException> exceptions = new ArrayList<>(destinations.size());
            for (String topic : destinations) {
                try {
                    this.sendCloudEvent(cloudEvent, topic, originalMessageId);
                } catch (MicoCloudEventException e) {
                    // defer exception
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
            cloudEvent = this.updateRouteHistoryWithTopic(cloudEvent, topic);
            // TODO commit logic/transactions
            setMissingHeaderFields(cloudEvent, originalMessageId);
            if (!isTestMessageCompleted(cloudEvent, topic)) {
                log.debug("Is not necessary to filter the message. Is test message '{}', filterOutBeforeTopic: '{}', targetTopic: '{}'", cloudEvent.isTestMessage(), cloudEvent.getFilterOutBeforeTopic(), topic);
                kafkaTemplate.send(topic, cloudEvent);
            } else {
                log.info("Filter out test message: '{}' to topic: '{}'", cloudEvent, kafkaConfig.getTestMessageOutputTopic());
                kafkaTemplate.send(kafkaConfig.getTestMessageOutputTopic(), cloudEvent);
            }
        } catch (Exception e) {
            throw new MicoCloudEventException("An error occured while sending the cloud event.", e, cloudEvent);
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
     * Sets the time, the correlationId and the Id field of a CloudEvent message if missing
     *
     * @param cloudEvent
     * @param originalMessageId
     */
    public void setMissingHeaderFields(MicoCloudEventImpl<JsonNode> cloudEvent, String originalMessageId) {
        if (StringUtils.isEmpty(cloudEvent.getId())) {
            cloudEvent.setRandomId();
            log.debug("Added missing id '{}' to cloud event", cloudEvent.getId());
        }
        if (!cloudEvent.getTime().isPresent()) {
            cloudEvent.setTime(ZonedDateTime.now());
            log.debug("Added missing time '{}' to cloud event", cloudEvent.getTime().orElse(null));
        }
        if (!StringUtils.isEmpty(originalMessageId)) {
            if (!cloudEvent.getCorrelationId().isPresent()) {
                cloudEvent.setCorrelationId(originalMessageId);
            }
            if (!cloudEvent.getId().equals(originalMessageId)) {
                if (!cloudEvent.isErrorMessage().orElse(false) ||
                    (cloudEvent.isErrorMessage().orElse(false) && StringUtils.isEmpty(cloudEvent.getCreatedFrom().orElse("")))) {
                    cloudEvent.setCreatedFrom(originalMessageId);
                }
            }
        }
        if (cloudEvent.isErrorMessage().orElse(false)) {
            try {
                URI source = new URI("kafka://" + this.kafkaConfig.getGroupId() + "/" + this.kafkaConfig.getInputTopic());
                cloudEvent.setSource(source);
            } catch (URISyntaxException e) {
                log.error("Could not construct a valid source attribute for the error message.", e);
            }
        }
    }
}
