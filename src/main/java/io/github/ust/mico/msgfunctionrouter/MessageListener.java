package io.github.ust.mico.msgfunctionrouter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.ust.mico.msgfunctionrouter.kafka.CloudEventExtensionImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZonedDateTime;

@Slf4j
@Component
public class MessageListener {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private KafkaTemplate<String, CloudEventExtensionImpl<JsonNode>> kafkaTemplate;
    @Value(value = "${kafka.outputTopic}")
    private String outputTopic;
    @Value(value = "${openfaas.gateway}")
    private String openFaasGateway;
    @Value(value = "${functions.dummy}")
    private String dummyFunction;

    @KafkaListener(topics = "${kafka.inputTopic}")
    public void receive(CloudEventExtensionImpl<JsonNode> cloudEvent) {
        log.info("Received CloudEvent message: {}", cloudEvent);
        if (!cloudEvent.getData().isPresent()) {
            log.warn("Received message does not include any data!");
        }

        URL functionUrl;
        try {
            URL gatewayUrl = new URL(openFaasGateway);
            functionUrl = new URL(gatewayUrl.getProtocol(), gatewayUrl.getHost(), gatewayUrl.getPort(),
                    gatewayUrl.getFile() + "/function/" + dummyFunction, null);
        } catch (MalformedURLException e) {
            log.error("Invalid URL to OpenFaaS gateway ({}) or function name ({}). Caused by: {}",
                    openFaasGateway, dummyFunction, e.getMessage());
            return;
        }
        String response = null;
        try {
            log.info("Start request to function '{}'", functionUrl.toString());
            String cloudEventSerialized = objectMapper.writeValueAsString(cloudEvent);
            log.debug("Serialized cloud event: {}", cloudEventSerialized);
            response = restTemplate.postForObject(functionUrl.toString(), cloudEventSerialized, String.class);
            log.info("OpenFaaS function response: {}", response);
            // TODO Error Handling -> Invalid Message Topic
            JsonNode payload = objectMapper.readTree(response);

            sendMessagePart(cloudEvent, payload);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize CloudEvent '{}'.", cloudEvent);
        } catch (IOException e) {
            log.error("Failed to parse JSON from response '{}'.", response);
        }
    }

    private void sendMessagePart(CloudEventExtensionImpl<JsonNode> cloudEvent, JsonNode jsonPart) {
        log.debug("Building message with content: {}", jsonPart.toString());
        //TODO add a better solution when https://github.com/cloudevents/sdk-java/issues/31 is fixed
        CloudEventExtensionImpl<JsonNode> cloudEventPart = new CloudEventExtensionImpl<>(cloudEvent.getType(), cloudEvent.getSpecVersion(),
                cloudEvent.getSource(), cloudEvent.getId(), cloudEvent.getTime().orElse(ZonedDateTime.now()), cloudEvent.getSchemaURL().orElse(null),
                cloudEvent.getContentType().orElse("application/json"), jsonPart, null);
        log.debug("Build CloudEvent:" + cloudEventPart.toString());
        kafkaTemplate.send(outputTopic, cloudEventPart);
    }
}
