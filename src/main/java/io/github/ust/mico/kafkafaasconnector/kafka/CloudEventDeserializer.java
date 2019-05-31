package io.github.ust.mico.kafkafaasconnector.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.cloudevents.json.Json;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class CloudEventDeserializer implements Deserializer<MicoCloudEventImpl<JsonNode>> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public MicoCloudEventImpl<JsonNode> deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            String message = new String(data, StandardCharsets.UTF_8);
            log.info("Trying to parse the message:" + message);
            return Json.decodeValue(message, new TypeReference<MicoCloudEventImpl<JsonNode>>() {
            });
        } catch (IllegalStateException e) {
            throw new SerializationException("Could not create an CloudEvent message", e);
        }
    }

    @Override
    public void close() {

    }
}
