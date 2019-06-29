package io.github.ust.mico.kafkafaasconnector.kafka;

import java.time.ZonedDateTime;
import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@JsonDeserialize(as = RouteHistory.class)
public class RouteHistory {

    private String type;
    private String id;
    private ZonedDateTime timestamp;

    public Optional<String> getType() {
        return Optional.ofNullable(this.type);
    }

    public Optional<String> getId() {
        return Optional.ofNullable(this.id);
    }

    public Optional<ZonedDateTime> getTimestamp() {
        return Optional.ofNullable(this.timestamp);
    }
}
