package io.github.ust.mico.kafkafaasconnector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.cloudevents.json.Json;
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

import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;

@Slf4j
@Component
public class MessageListener {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    protected static final String CONTENT_TYPE = "application/json";

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
        log.debug("Received CloudEvent message: {}", cloudEvent);
        if (!cloudEvent.getData().isPresent()) {
            // data is entirely optional
            log.debug("Received message does not include any data!");
        }
        if (this.openFaaSConfig.isSkipFunctionCall()) {
            // when skipping the openFaaS function just pass on the original cloudEvent
            this.sendCloudEvent(cloudEvent);
        } else {
            String functionResult = callFaasFunction(cloudEvent);
            ArrayList<MicoCloudEventImpl<JsonNode>> events = parseFunctionResult(functionResult, cloudEvent);
            events.forEach(this::sendCloudEvent);
        }
    }

    public MicoCloudEventImpl<JsonNode> updateRouteHistoryWithFunctionCall(MicoCloudEventImpl<JsonNode> cloudEvent, String functionId) {
        return this.updateRouteHistory(cloudEvent, functionId, "topic");
    }

    public MicoCloudEventImpl<JsonNode> updateRouteHistoryWithTopic(MicoCloudEventImpl<JsonNode> cloudEvent, String topic) {
        return this.updateRouteHistory(cloudEvent, topic, "topic");
    }

    public MicoCloudEventImpl<JsonNode> updateRouteHistory(MicoCloudEventImpl<JsonNode> cloudEvent, String step, String type) {
        // TODO update route history here
        return cloudEvent;
    }

    public String callFaasFunction(MicoCloudEventImpl<JsonNode> cloudEvent) {
        try {
            URL functionUrl = openFaaSConfig.getFunctionUrl();
            log.debug("Start request to function '{}'", functionUrl.toString());
            String cloudEventSerialized = objectMapper.writeValueAsString(this.updateRouteHistoryWithFunctionCall(cloudEvent, openFaaSConfig.getFunctionName()));
            log.debug("Serialized cloud event: {}", cloudEventSerialized);
            return restTemplate.postForObject(functionUrl.toString(), cloudEventSerialized, String.class);
        } catch (MalformedURLException e) {
            // TODO decide error behavoiur and commit behaviour
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize CloudEvent '{}'.", cloudEvent);
            sendErrorMessageToInvalidMessageTopic("Failed to serialize CloudEvent: " + cloudEvent.toString(), cloudEvent);
        }
        return null;
    }

    public ArrayList<MicoCloudEventImpl<JsonNode>> parseFunctionResult(String functionResult, MicoCloudEventImpl<JsonNode> sourceCloudEvent) {
        try {
            // TODO Error Handling -> Invalid Message Topic
            return Json.decodeValue(functionResult, new TypeReference<ArrayList<MicoCloudEventImpl<JsonNode>>>() {});
            //sendErrorMessageToInvalidMessageTopic("Return of FaaS function is not an array:" + functionResult, sourceCloudEvent);
            //Maybe move the kafka invalid message topic handling to a log appender
        } catch (IllegalStateException e) {
            log.error("Failed to parse JSON from response '{}'.", functionResult);
            sendErrorMessageToInvalidMessageTopic("Failed to parse JSON from response: " + functionResult, sourceCloudEvent);
        } catch (HttpStatusCodeException e){
            log.error("A client error occurred with http status:{} . These exceptions are triggered if the  FaaS function does not return 200 OK as the status code", e.getStatusCode(),e);
            sendErrorMessageToInvalidMessageTopic(e.toString(),sourceCloudEvent);
        }
        // TODO refactor error reporting to use exceptions similar to exception error
        // reporting in mico core api (see HttpStatusCodeException)
        return null;
    }

    public void sendCloudEvent(MicoCloudEventImpl<JsonNode> cloudEvent) {
        // TODO call routing slip logic here

        // default case:
        this.sendCloudEvent(cloudEvent, this.kafkaConfig.getOutputTopic());
    }

    private void sendCloudEvent(MicoCloudEventImpl<JsonNode> cloudEvent, String topic) {
        cloudEvent = this.updateRouteHistoryWithTopic(cloudEvent, topic);
        // TODO commit logic/transactions
        kafkaTemplate.send(topic, cloudEvent);
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
        kafkaTemplate.send(kafkaConfig.getInvalidMessageTopic(), cloudEventErrorReportMessage);
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
