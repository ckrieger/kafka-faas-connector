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

import io.github.ust.mico.kafkafaasconnector.MessageListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
@Slf4j
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.embedded.kafka.brokers}")
    private String embeddedBootstrapServers;

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> properties = new HashMap<>();
        if (embeddedBootstrapServers != null) {
            // give priority to embedded kafka
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    embeddedBootstrapServers);
        } else {
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    bootstrapServers);
        }
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer2.class);
        properties.put(ErrorHandlingDeserializer2.KEY_DESERIALIZER_CLASS,
                StringDeserializer.class);
        //https://docs.spring.io/spring-kafka/docs/2.2.0.RELEASE/reference/html/_reference.html#error-handling-deserializer
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer2.class);
        properties.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS,
                CloudEventDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "TestGroup");

        return properties;
    }

    @Bean
    public ConsumerFactory<String, MicoCloudEventImpl> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, MicoCloudEventImpl>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MicoCloudEventImpl> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setErrorHandler(new SeekToCurrentErrorHandler(1));
        //TODO Add DeadLetterPublishingRecoverer later
        return factory;
    }

    @Bean
    public MessageListener receiver() {
        return new MessageListener();
    }
}
