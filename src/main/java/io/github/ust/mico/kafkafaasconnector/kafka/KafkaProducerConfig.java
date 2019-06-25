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

import com.fasterxml.jackson.databind.JsonNode;
import io.github.ust.mico.kafkafaasconnector.configuration.KafkaConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Autowired
    private KafkaConfig kafkaConfig;

    @Bean
    public ProducerFactory<String, MicoCloudEventImpl<JsonNode>> producerFactory() {
        Map<String, Object> configProps = putConfig();
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public ProducerFactory<Object, Object> invalidMessageProducerFactory() {
        Map<String, Object> configProps = putConfig();
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    private Map<String, Object> putConfig() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            kafkaConfig.getBootstrapServers());
        configProps.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class);
        configProps.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            CloudEventSerializer.class);
        return configProps;
    }

    @Bean
    public KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public KafkaTemplate<Object, Object> invalidMessageTemplate() {
        return new KafkaTemplate<>(invalidMessageProducerFactory());
    }
}
