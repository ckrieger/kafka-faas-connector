package io.github.ust.mico.kafkafaasconnector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.ust.mico.kafkafaasconnector.configuration.KafkaConfig;
import io.github.ust.mico.kafkafaasconnector.configuration.OpenFaaSConfig;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import io.github.ust.mico.kafkafaasconnector.kafka.ErrorReportMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.UUID;

@Slf4j
@Component
public class MessageListener {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    protected static final String CONTENT_TYPE = "application/json";
    protected static final String INVALID_MESSAGE_TOPIC = "InvalidMessage";

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> kafkaTemplate;

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private OpenFaaSConfig openFaaSConfig;

    @KafkaListener(topics = "${kafka.input-topic}")
    public void receive(MicoCloudEventImpl<JsonNode> cloudEvent) {
        log.info("Received CloudEvent message: {}", cloudEvent);
        if (!cloudEvent.getData().isPresent()) {
            log.warn("Received message does not include any data!");
        }

        URL functionUrl;
        try {
            URL gatewayUrl = new URL(openFaaSConfig.getGateway());
            functionUrl = new URL(gatewayUrl.getProtocol(), gatewayUrl.getHost(), gatewayUrl.getPort(),
                    gatewayUrl.getFile() + "/function/" + openFaaSConfig.getFunctionName(), null);
        } catch (MalformedURLException e) {
            log.error("Invalid URL to OpenFaaS gateway ({}) or function name ({}). Caused by: {}",
                    openFaaSConfig.getGateway(), openFaaSConfig.getFunctionName(), e.getMessage());
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
            if(payload.isArray()){
                //Routing slip implementation here
                log.info("Return of FaaS is an array");
            }else{
                sendErrorMessageToInvalidMessageTopic("Return of FaaS function is not an array:" + response,cloudEvent);
            }
            //Maybe move the kafka invalid message topic handling to a log appender
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize CloudEvent '{}'.", cloudEvent);
            sendErrorMessageToInvalidMessageTopic("Failed to serialize CloudEvent: " + cloudEvent.toString(),cloudEvent);
        } catch (IOException e) {
            log.error("Failed to parse JSON from response '{}'.", response);
            sendErrorMessageToInvalidMessageTopic("Failed to parse JSON from response: " + response,cloudEvent);
        } catch (HttpStatusCodeException e){
            log.error("A client error occurred with http status:{} . These exceptions are triggered if the  FaaS function does not return 200 OK as the status code", e.getStatusCode(),e);
            sendErrorMessageToInvalidMessageTopic(e.toString(),cloudEvent);

        }
    }

    private void sendErrorMessageToInvalidMessageTopic(String errorMessage, MicoCloudEventImpl<JsonNode> cloudEvent) {
        sendErrorMessage(errorMessage,cloudEvent,kafkaConfig.getInvalidMessageTopic());
    }

    private void sendErrorMessageToDeadLetterTopic(String errorMessage, MicoCloudEventImpl<JsonNode> cloudEvent) {
        sendErrorMessage(errorMessage,cloudEvent,kafkaConfig.getDeadLetterTopic());
    }

    private void sendErrorMessage(String errorMessage, MicoCloudEventImpl<JsonNode> cloudEvent, String topic) {
        log.error(errorMessage);
        MicoCloudEventImpl<JsonNode> cloudEventErrorReportMessage = getCloudEventErrorReportMessage(errorMessage,cloudEvent.getId());
        kafkaTemplate.send(INVALID_MESSAGE_TOPIC,cloudEventErrorReportMessage);
    }


    private MicoCloudEventImpl<JsonNode> getCloudEventErrorReportMessage(String errorMessage, String originalMessageId) {
        ErrorReportMessage errorReportMessage = getErrorReportMessage(errorMessage, originalMessageId);
        return new MicoCloudEventImpl<JsonNode>()
                .setContentType(CONTENT_TYPE)
                .setRandomId()
                .setTime(ZonedDateTime.now())
                .setData(objectMapper.valueToTree(errorReportMessage))
                .setType(ErrorReportMessage.class.getName());
    }

    /**
     * Generates a error report message for a given message. It adds the input topic, the output topic, the function name,
     * the function gateway and the name of this component as metadata to the message.
     * @param message the error message
     * @return a {@link ErrorReportMessage} which contains meta data about the used configuration
     */
    private ErrorReportMessage getErrorReportMessage(String message, String originalMessageId) {
        return new ErrorReportMessage()
                .setErrorMessage(message)
                .setOriginalMessageId(originalMessageId)
                .setInputTopic(kafkaConfig.getInputTopic())
                .setOutputTopic(kafkaConfig.getOutputTopic())
                .setFaasFunctionName(openFaaSConfig.getFunctionName())
                .setFaasGateway(openFaaSConfig.getGateway())
                .setReportingComponentName("TODO"); //TODO set this via a env variable and use it here
    }
}
