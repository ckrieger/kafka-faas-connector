package io.github.ust.mico.kafkafaasconnector.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotBlank;

/**
 * Configuration of the kafka connection.
 */
@Component
@Setter
@Getter
@ConfigurationProperties("kafka")
public class KafkaConfig {

    /**
     * The URL of the Kafka bootstrap server.
     */
    @NotBlank
    private String bootstrapServer;

    /**
     * The Kafka input topic.
     */
    @NotBlank
    private String inputTopic;

    /**
     * The Kafka output topic.
     */
    @NotBlank
    private String outputTopic;

    /**
     * Used to report message processing errors
     */
    @NotBlank
    private String invalidMessageTopic;

    /**
     * Used to report routing errors
     */
    @NotBlank
    private String deadLetterTopic;


    @NotBlank
    private String filteredTestMessagesTopic;
}
