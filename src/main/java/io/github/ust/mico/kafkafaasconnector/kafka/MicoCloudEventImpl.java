package io.github.ust.mico.kafkafaasconnector.kafka;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.cloudevents.CloudEvent;
import io.cloudevents.Extension;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@JsonDeserialize(as = MicoCloudEventImpl.class)
public class MicoCloudEventImpl<T> implements CloudEvent<T> {

    protected static final String SPEC_VERSION = "0.2";

    //mandatory
    private String id;
    private URI source;
    private String type;
    @JsonAlias({"specversion"})
    private String specVersion = SPEC_VERSION;


    //Optional
    private ZonedDateTime time;
    private URI schemaURL;
    private String contentType;
    private T data;
    private List<Extension> extensions;

    private String correlationId;
    private String createFrom;
    private String route;
    private List<String> routingSlip;
    private boolean isTestMessage;
    private String filterOutBeforeTopic;
    private ZonedDateTime expiryDate;
    private String sequenceId;
    private String sequenceNumber;
    private String sequenceSize;
    private String returnTopic;
    private String dataRef;


    public Optional<ZonedDateTime> getTime(){
        return Optional.ofNullable(time);
    }

    public Optional<URI> getSchemaURL(){
        return Optional.ofNullable(schemaURL);
    }

    public Optional<String> getContentType(){
        return Optional.ofNullable(contentType);
    }

    public Optional<T> getData(){
        return Optional.ofNullable(data);
    }

    public Optional<List<Extension>> getExtensions(){
        return Optional.ofNullable(extensions);
    }

    public Optional<String> getCorrelationId() {
        return Optional.ofNullable(correlationId);
    }

    public Optional<String> getCreateFrom() {
        return Optional.ofNullable(createFrom);
    }

    public Optional<String> getRoute() {
        return Optional.ofNullable(route);
    }

    public Optional<List<String>> getRoutingSlip() {
        return Optional.ofNullable(routingSlip);
    }

    public Optional<Boolean> isTestMessage() {
        return Optional.ofNullable(isTestMessage);
    }

    public Optional<String> getFilterOutBeforeTopic() {
        return Optional.ofNullable(filterOutBeforeTopic);
    }

    public Optional<ZonedDateTime> getExpiryDate() {
        return Optional.ofNullable(expiryDate);
    }

    public Optional<String> getSequenceId() {
        return Optional.ofNullable(sequenceId);
    }

    public Optional<String> getSequenceNumber() {
        return Optional.ofNullable(sequenceNumber);
    }

    public Optional<String> getSequenceSize() {
        return Optional.ofNullable(sequenceSize);
    }

    public Optional<String> getReturnTopic() {
        return Optional.ofNullable(returnTopic);
    }

    public Optional<String> getDataRef() {
        return Optional.ofNullable(dataRef);
    }
}
