package io.github.ust.mico.msgfunctionrouter.kafka;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.cloudevents.Extension;
import io.cloudevents.impl.DefaultCloudEventImpl;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Is a hack to enable extensions for serialization, wait for https://github.com/cloudevents/sdk-java/issues/31
 */
@JsonDeserialize(as = CloudEventExtensionImpl.class)
public class CloudEventExtensionImpl<T> extends DefaultCloudEventImpl<T> {

    public CloudEventExtensionImpl(String type, String specversion, URI source, String id, ZonedDateTime time, URI schemaURL, String contentType, T data, List extensions) {
        super(type, specversion, source, id, time, schemaURL, contentType, data, extensions);
    }

    public CloudEventExtensionImpl() {
        super(null, null, null, null, null, null, null, null, null);
    }

    @JsonAnyGetter
    public Map<String, Object> getExtendedAttributes() {
        Map<String, Object> extendedAttributes = new HashMap<>();
        for (Extension extension : this.getExtensions().orElse(new ArrayList<>())) {
            for (Field field : extension.getClass().getDeclaredFields()) {
                try {
                    field.setAccessible(true);
                    Object value = field.get(extension);
                    extendedAttributes.put(field.getName(), value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return extendedAttributes;
    }


}
