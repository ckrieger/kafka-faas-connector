package io.github.ust.mico.kafkafaasconnector;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.ust.mico.kafkafaasconnector.configuration.KafkaConfig;
import io.github.ust.mico.kafkafaasconnector.kafka.CloudEventDeserializer;
import io.github.ust.mico.kafkafaasconnector.kafka.CloudEventSerializer;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class MicoKafkaTestUtils {

    /**
     * Get a kafka consumer for testing with an embedded broker.
     * <p>
     * The consumer needs to be unsubscribed after the test is finished.
     * If not other tests using the same broker may fail to get messages!
     *
     * @param embeddedKafka the embedded kafka broker
     * @return
     */
    public static Consumer<String, MicoCloudEventImpl<JsonNode>> getKafkaConsumer(EmbeddedKafkaBroker embeddedKafka) {
        Map<String, Object> consumerProps = org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps("testT", "false", embeddedKafka);
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
     * Unsubscribes and closes the consumer. Needed after each test.
     * @param consumer
     */
    public static void unsubscribeConsumer(Consumer consumer){
        consumer.unsubscribe();
        consumer.close();
    }

    /**
     * Get a kafka producer (KafkaTemplate) for testing with an embedded broker.
     *
     * @param embeddedKafka the embedded kafka broker
     * @return
     */
    public static KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> getKafkaProducer(EmbeddedKafkaBroker embeddedKafka) {
        Map<String, Object> producerProps = org.springframework.kafka.test.utils.KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class);
        producerProps.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            CloudEventSerializer.class);
        DefaultKafkaProducerFactory<String, MicoCloudEventImpl<JsonNode>> pf = new DefaultKafkaProducerFactory<String, MicoCloudEventImpl<JsonNode>>(producerProps);
        return new KafkaTemplate<>(pf);
    }

    /**
     * This method requests the topics which are acctually used by the broker.
     * It is nesseary because embeddedKafka.getTopics() does not contain a recent topic list
     * @param embeddedKafka
     * @return
     */
    public static Set<String> requestActuallySetTopics(EmbeddedKafkaBroker embeddedKafka) {
        Set<String> topics = new HashSet<>();
        embeddedKafka.doWithAdmin(admin -> {
            try {
                topics.addAll(admin.listTopics().names().get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
        return topics;
    }

    /**
     * Contains all topics which are necessary for the tests
     * @return
     */
    public static Set<String> getRequiredTopics(KafkaConfig kafkaConfig) {
        Set<String> requiredTopics = new HashSet<>();
        requiredTopics.addAll(Arrays.asList(
            kafkaConfig.getTestMessageOutputTopic(),
            kafkaConfig.getDeadLetterTopic(),
            kafkaConfig.getInvalidMessageTopic(),
            kafkaConfig.getInputTopic(),
            kafkaConfig.getOutputTopic(),
            TestConstants.ROUTING_TOPIC_1,
            TestConstants.ROUTING_TOPIC_2,
            TestConstants.ROUTING_TOPIC_3,
            TestConstants.ROUTING_TOPIC_4));
        return requiredTopics;
    }

    /**
     * Generates a consumer based on the given topics
     * @return
     */
    public static Consumer<String, MicoCloudEventImpl<JsonNode>> getKafkaConsumer(EmbeddedKafkaBroker embeddedKafka, String... topics) {
        Consumer<String, MicoCloudEventImpl<JsonNode>> consumer = MicoKafkaTestUtils.getKafkaConsumer(embeddedKafka);
        embeddedKafka.consumeFromEmbeddedTopics(consumer, topics);
        return consumer;
    }
}
