package io.github.ust.mico.kafkafaasconnector;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.ust.mico.kafkafaasconnector.configuration.KafkaConfig;
import io.github.ust.mico.kafkafaasconnector.exception.BatchMicoCloudEventException;
import io.github.ust.mico.kafkafaasconnector.exception.MicoCloudEventException;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import io.github.ust.mico.kafkafaasconnector.kafka.RouteHistory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
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
     * Add a topic routing step to the routing history of the cloud event.
     *
     * @param cloudEvent the cloud event to update
     * @param topic      the next topic the event will be sent to
     * @return the updated cloud event
     */
    private MicoCloudEventImpl<JsonNode> updateRouteHistoryWithTopic(MicoCloudEventImpl<JsonNode> cloudEvent, String topic) {
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
     * Sets the time, the correlationId and the Id field of a CloudEvent message if missing
     *
     * @param cloudEvent        the cloud event to send
     * @param originalMessageId the id of the original message
     */
    public void setMissingHeaderFields(MicoCloudEventImpl<JsonNode> cloudEvent, String originalMessageId) {
        // Add random id if missing
        if (StringUtils.isEmpty(cloudEvent.getId())) {
            cloudEvent.setRandomId();
            log.debug("Added missing id '{}' to cloud event", cloudEvent.getId());
        }
        // Add time if missing
        if (!cloudEvent.getTime().isPresent()) {
            cloudEvent.setTime(ZonedDateTime.now());
            log.debug("Added missing time '{}' to cloud event", cloudEvent.getTime().orElse(null));
        }
        if (!StringUtils.isEmpty(originalMessageId)) {
            // Add correlation id if missing
            if (!cloudEvent.getCorrelationId().isPresent()) {
                cloudEvent.setCorrelationId(originalMessageId);
            }
            // Set 'created from' to the original message id only if necessary
            if (!cloudEvent.getId().equals(originalMessageId)) {
                if (!cloudEvent.isErrorMessage().orElse(false) ||
                    (cloudEvent.isErrorMessage().orElse(false) && StringUtils.isEmpty(cloudEvent.getCreatedFrom().orElse("")))) {
                    cloudEvent.setCreatedFrom(originalMessageId);
                }
            }
        }
        // Add source if it is an error message, e.g.: kafka://mico/transform-request
        if (cloudEvent.isErrorMessage().orElse(false)) {
            try {
                URI source = new URI("kafka://" + this.kafkaConfig.getGroupId() + "/" + this.kafkaConfig.getInputTopic());
                cloudEvent.setSource(source);
            } catch (URISyntaxException e) {
                log.error("Could not construct a valid source attribute for the error message. " +
                    "Caused by: {}", e.getMessage());
            }
        }
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
