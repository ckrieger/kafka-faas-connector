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
package io.github.ust.mico.kafkafaasconnector;


import io.github.ust.mico.kafkafaasconnector.kafka.CloudEventDeserializer;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

import java.nio.charset.Charset;

public class CloudEventDeserializerTests {

    /**
     * Tests message deserialization with a broken message
     */
    @Test(expected = SerializationException.class)
    public void testBrokenMessageDeserialization(){
        CloudEventDeserializer cloudEventDeserializer = new CloudEventDeserializer();
        String invalidMessage = "InvalidMessage";
        cloudEventDeserializer.deserialize("",invalidMessage.getBytes(Charset.defaultCharset()));
    }

    /**
     * Tests message serialization with a empty but not null message
     */
    @Test(expected = SerializationException.class)
    public void testEmptyMessageSerialization(){
        CloudEventDeserializer cloudEventDeserializer = new CloudEventDeserializer();
        byte[] message = {};
        cloudEventDeserializer.deserialize("",message);
    }
}
