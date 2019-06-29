package io.github.ust.mico.kafkafaasconnector.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import io.cloudevents.json.Json;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class CloudEventSerializer implements Serializer<MicoCloudEventImpl<JsonNode>> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, MicoCloudEventImpl<JsonNode> data) {
        if (data == null)
            return null;
        else {
            String eventAsString = Json.encode(data);
            byte[] eventAsBytes = eventAsString.getBytes(StandardCharsets.UTF_8);
            log.debug("Serializing the event:'{}' to '{}'", data, eventAsString);
            return eventAsBytes;
        }
    }

    @Override
    public void close() {

    }
}
