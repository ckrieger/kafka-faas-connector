package io.github.ust.mico.kafkafaasconnector.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ErrorReportMessage {

    String errorMessage;
    String originalMessageId; // Replace with createdfrom in the CloudEvent envelope
    String inputTopic;
    String outputTopic;
    String faasGateway;
    String faasFunctionName;
    String reportingComponentName;
}
