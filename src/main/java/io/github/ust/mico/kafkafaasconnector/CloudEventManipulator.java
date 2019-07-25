package io.github.ust.mico.kafkafaasconnector;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.ust.mico.kafkafaasconnector.configuration.KafkaConfig;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import io.github.ust.mico.kafkafaasconnector.kafka.RouteHistory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class CloudEventManipulator {

    private static final String ROUTE_HISTORY_TYPE_TOPIC = "topic";
    private static final String ROUTE_HISTORY_TYPE_FAAS_FUNCTION = "faas-function";

    @Autowired
    private KafkaConfig kafkaConfig;

    /**
     * Add a topic routing step to the routing history of the cloud event.
     *
     * @param cloudEvent the cloud event to update
     * @param topic      the next topic the event will be sent to
     * @return the updated cloud event
     */
    public MicoCloudEventImpl<JsonNode> updateRouteHistoryWithTopic(MicoCloudEventImpl<JsonNode> cloudEvent, String topic) {
        return this.updateRouteHistory(cloudEvent, topic, ROUTE_HISTORY_TYPE_TOPIC);
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
        List<RouteHistory> history = cloudEvent.getRoute().map(ArrayList::new).orElse(new ArrayList<>());
        history.add(routingStep);
        return new MicoCloudEventImpl<>(cloudEvent).setRoute(history);
    }

    /**
     * Add a function call routing step to the routing history of the cloud event.
     *
     * @param cloudEvent the cloud event to update
     * @param functionId the id of the function applied to the cloud event next
     * @return the updated cloud event
     */
    public MicoCloudEventImpl<JsonNode> updateRouteHistoryWithFunctionCall(MicoCloudEventImpl<JsonNode> cloudEvent, String functionId) {
        return this.updateRouteHistory(cloudEvent, functionId, ROUTE_HISTORY_TYPE_FAAS_FUNCTION);
    }


    /**
     * Sets the time, the correlationId and the Id field of a CloudEvent message if missing
     *
     * @param cloudEvent        the cloud event to send
     * @param originalMessageId the id of the original message
     */
    public void setMissingHeaderFields(MicoCloudEventImpl<JsonNode> cloudEvent, String originalMessageId) {

        setMissingId(cloudEvent);
        addMissingTime(cloudEvent);

        if (!StringUtils.isEmpty(originalMessageId)) {
            setMissingCorrelationId(cloudEvent, originalMessageId);
            setMissingCreatedFrom(cloudEvent, originalMessageId);
        }

        // Add source if it is an error message, e.g.: kafka://mico/transform-request
        if (cloudEvent.isErrorMessage().orElse(false)) {
            setMissingSource(cloudEvent);
        }
    }

    /**
     * Sets the source field of an cloud event message to "kafka://{groupId}/{inputTopic}".
     * @param cloudEvent
     */
    private void setMissingSource(MicoCloudEventImpl<JsonNode> cloudEvent) {
        try {
            URI source = new URI("kafka://" + this.kafkaConfig.getGroupId() + "/" + this.kafkaConfig.getInputTopic());
            cloudEvent.setSource(source);
        } catch (URISyntaxException e) {
            log.error("Could not construct a valid source attribute for the error message. " +
                "Caused by: {}", e.getMessage());
        }
    }

    /**
     * Adds the createdFrom field to the message if the messageId is different from the originalMessageId and
     * the createdFrom field is empty.
     * @param cloudEvent
     * @param originalMessageId
     */
    private void setMissingCreatedFrom(MicoCloudEventImpl<JsonNode> cloudEvent, String originalMessageId) {
        if (!cloudEvent.getId().equals(originalMessageId)) {
            if (!cloudEvent.isErrorMessage().orElse(false) ||
                (cloudEvent.isErrorMessage().orElse(false) && StringUtils.isEmpty(cloudEvent.getCreatedFrom().orElse("")))) {
                cloudEvent.setCreatedFrom(originalMessageId);
            }
        }
    }

    /**
     * Sets the message correlationId to the originalMessageId if the correlationId is missing
     * @param cloudEvent
     * @param originalMessageId
     */
    private void setMissingCorrelationId(MicoCloudEventImpl<JsonNode> cloudEvent, String originalMessageId) {
        if (!cloudEvent.getCorrelationId().isPresent()) {
            cloudEvent.setCorrelationId(originalMessageId);
        }
    }

    /**
     * Adds the required field 'time' if it is missing.
     * @param cloudEvent
     */
    private void addMissingTime(MicoCloudEventImpl<JsonNode> cloudEvent) {
        if (!cloudEvent.getTime().isPresent()) {
            cloudEvent.setTime(ZonedDateTime.now());
            log.debug("Added missing time '{}' to cloud event", cloudEvent.getTime().orElse(null));
        }
    }

    /**
     * Sets a missing message id to a randomly generated one.
     * @param cloudEvent
     */
    private void setMissingId(MicoCloudEventImpl<JsonNode> cloudEvent) {
        if (StringUtils.isEmpty(cloudEvent.getId())) {
            cloudEvent.setRandomId();
            log.debug("Added missing id '{}' to cloud event", cloudEvent.getId());
        }
    }


}
