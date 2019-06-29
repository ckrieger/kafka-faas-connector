/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
            MicoCloudEventImpl<JsonNode> micoCloudEvent = Json.decodeValue(message, new TypeReference<MicoCloudEventImpl<JsonNode>>() {
            });
            log.info("Deserialized micoCloudEvent '{}' on topic: '{}'",micoCloudEvent.toString(),topic);
            return micoCloudEvent;
        } catch (IllegalStateException e) {
            throw new SerializationException("Could not create an CloudEvent message", e);
        }
    }

    @Override
    public void close() {

    }
}
