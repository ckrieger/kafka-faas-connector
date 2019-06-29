package io.github.ust.mico.kafkafaasconnector;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import io.github.ust.mico.kafkafaasconnector.kafka.CloudEventDeserializer;
import io.github.ust.mico.kafkafaasconnector.kafka.CloudEventSerializer;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public class TestConstants {

    // Kafka topics
    public static String ROUTING_TOPIC_1 = "route-1";
    public static String ROUTING_TOPIC_2 = "route-2";
    public static String ROUTING_TOPIC_3 = "route-3";
    public static String ROUTING_TOPIC_4 = "route-4";

    // CloudEvents

    /**
     * Build a basic cloud event with dummy entries and with the given id.
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
     * Build a basic cloud event with dummy entries and a random id.
     *
     */
    public static MicoCloudEventImpl<JsonNode> basicCloudEventWithRandomId() {
            return basicCloudEvent("").setRandomId();
    }

    /**
     * Mark the given cloud event as test message.
     *
     * @param message              the cloud event to mark as test message
     * @param filterOutBeforeTopic filter out the message before this topic
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> markAsTestMessage(MicoCloudEventImpl<JsonNode> message, String filterOutBeforeTopic) {
        return message.setIsTestMessage(true).setFilterOutBeforeTopic(filterOutBeforeTopic);
    }

    /**
     * Add a single topic routing step to the message.
     * <p>
     * Create a new routing slip if no present. Appends topic to routing slip.
     *
     * @param message the message to edit
     * @param topic   the topic to add to the routing slip
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> addSingleTopicRoutingStep(MicoCloudEventImpl<JsonNode> message, String topic) {
        ArrayList<String> destinations = new ArrayList<>();
        destinations.add(topic);
        return addMultipleTopicRoutingSteps(message, destinations);
    }

    /**
     * Add multiple topics as a Array to the message
     * <p>
     * Create a new routing slip if none are found. Appends topics to new routing slip
     *
     * @param message
     * @param destinations
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> addMultipleTopicRoutingSteps(MicoCloudEventImpl<JsonNode> message, List<String> destinations) {
        Optional<List<List<String>>> routingSlip = message.getRoutingSlip();
        List<List<String>> newRoutingSlip = routingSlip.orElse(new ArrayList<>());
        newRoutingSlip.add(destinations);
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
     * @param message        the message to mark as part of a sequence
     * @param sequenceId     the sequence id
     * @param sequenceNumber the number this message has in the sequence (1-based index)
     * @param sequenceSize   the total number of messages in the sequence
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> setSequenceAttributes(MicoCloudEventImpl<JsonNode> message, String sequenceId, int sequenceNumber, int sequenceSize) {
        return message.setSequenceId(sequenceId)
            .setSequenceSize(sequenceSize)
            .setSequenceNumber(sequenceNumber);
    }

    // Kafka

    /**
     * Get a kafka consumer for testing with an embedded broker.
     *
     * The consumer needs to be unsubscribed after the test is finished.
     * If not other tests using the same broker may fail to get messages!
     *
     * @param embeddedKafka the embedded kafka broker
     * @return
     */
    public static Consumer<String, MicoCloudEventImpl<JsonNode>> getKafkaConsumer(EmbeddedKafkaBroker embeddedKafka) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testT", "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer2.class);
        consumerProps.put(ErrorHandlingDeserializer2.KEY_DESERIALIZER_CLASS,
                StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer2.class);
        consumerProps.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS,
                CloudEventDeserializer.class);
        DefaultKafkaConsumerFactory<String, MicoCloudEventImpl<JsonNode>> cf = new DefaultKafkaConsumerFactory<String, MicoCloudEventImpl<JsonNode>>(consumerProps);
        return cf.createConsumer();
    }

    /**
     * Get a kafka producer (KafkaTemplate) for testing with an embedded broker.
     *
     * @param embeddedKafka the embedded kafka broker
     * @return
     */
    public static KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> getKafkaProducer(EmbeddedKafkaBroker embeddedKafka) {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        producerProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                CloudEventSerializer.class);
        DefaultKafkaProducerFactory<String, MicoCloudEventImpl<JsonNode>> pf = new DefaultKafkaProducerFactory<String, MicoCloudEventImpl<JsonNode>>(producerProps);
        return new KafkaTemplate<>(pf);
    }

}
