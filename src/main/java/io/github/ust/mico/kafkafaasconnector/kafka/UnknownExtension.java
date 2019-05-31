package io.github.ust.mico.kafkafaasconnector.kafka;

import io.cloudevents.Extension;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class UnknownExtension implements Extension {
    String key;
    String value;
}
