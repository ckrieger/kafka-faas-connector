package io.github.ust.mico.kafkafaasconnector;


import io.github.ust.mico.kafkafaasconnector.kafka.CloudEventDeserializer;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

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
