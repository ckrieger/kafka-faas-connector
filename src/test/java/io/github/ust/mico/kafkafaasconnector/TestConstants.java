package io.github.ust.mico.kafkafaasconnector;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class TestConstants {

    // Kafka topics
    public static String INPUT_TOPIC = "input";
    public static String OUTPUT_TOPIC = "output";
    public static String INVALID_MESSAGE_TOPIC = "InvalidMessage";
    public static String DEAD_LETTER_TOPIC = "DeadLetter";
    public static String ROUTING_TOPIC_1 = "route-1";
    public static String ROUTING_TOPIC_2 = "route-2";
    public static String ROUTING_TOPIC_3 = "route-3";
    public static String ROUTING_TOPIC_4 = "route-4";

    // CloudEvents
    /**
     * Build a basic cloud event wiht dummy entries and with the given id.
     *
     * @param id the cloud event id to use
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> basicCloudEvent(String id) {
        try {
            URI uri = new URI("http://example.com/unit-test");
            return new MicoCloudEventImpl<JsonNode>()
                .setId(id)
                .setSource(uri)
                .setType("UnitTestMessage")
                .setTime(ZonedDateTime.now());
        } catch (URISyntaxException e) {
            // Should never happen => no real error handling
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Mark the given cloud event as test message.
     *
     * @param message the cloud event to mark as test message
     * @param filterOutBeforeTopic filter out the message before this topic
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> markAsTestMessage(MicoCloudEventImpl<JsonNode> message, String filterOutBeforeTopic) {
        return message.setTestMessage(true).setFilterOutBeforeTopic(filterOutBeforeTopic);
    }

    /**
     * Add a single topic routing step to the message.
     *
     * Create a new routing slip if no present. Appends topic to routing slip.
     *
     * @param message the message to edit
     * @param topic the topic to add to the routing slip
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> addSingleTopicRoutingStep(MicoCloudEventImpl<JsonNode> message, String topic) {
        Optional<List<String>> routingSlip = message.getRoutingSlip();
        List<String> newRoutingSlip = routingSlip.orElse(new ArrayList<String>());
        newRoutingSlip.add(topic);
        return message.setRoutingSlip(newRoutingSlip);
    }

    /**
     * Set a already passed expiry date.
     *
     * @param message the message to edit
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> setPastExpiryDate(MicoCloudEventImpl<JsonNode> message) {
        return message.setExpiryDate(ZonedDateTime.now().minusHours(1));
    }

    /**
     * Set a future date as expiry date.
     *
     * @param message the message to edit
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> setFutureExpiryDate(MicoCloudEventImpl<JsonNode> message) {
        return message.setExpiryDate(ZonedDateTime.now().plusHours(1));
    }

    /**
     * Mark the message as part of a sequence.
     *
     * @param message the message to mark as part of a sequence
     * @param sequenceId the sequence id
     * @param sequenceNumber the number this message has in the sequence (1-based index)
     * @param sequenceSize the total number of messages in the sequence
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> setSequenceAttributes(MicoCloudEventImpl<JsonNode> message, String sequenceId, int sequenceNumber, int sequenceSize) {
        return message.setSequenceId(sequenceId)
            .setSequenceSize(String.valueOf(sequenceSize))
            .setSequenceNumber(String.valueOf(sequenceNumber));
    }

}