package io.github.ust.mico.kafkafaasconnector;

import io.github.ust.mico.kafkafaasconnector.configuration.KafkaConfig;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TestUtils {

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
}
