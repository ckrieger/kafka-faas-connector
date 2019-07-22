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

package io.github.ust.mico.kafkafaasconnector.exception;

import com.fasterxml.jackson.databind.JsonNode;
import io.cloudevents.json.Json;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Optional;

/**
 * Base exception for CloudEvent errors.
 */
@Slf4j
public class MicoCloudEventException extends Exception {

    private static final long serialVersionUID = 7526812870651753814L;

    /**
     * The CloudEvent that produced this error.
     */
    private final MicoCloudEventImpl<JsonNode> sourceEvent;

    public MicoCloudEventException(MicoCloudEventImpl<JsonNode> sourceEvent) {
        super();
        this.sourceEvent = sourceEvent;
    }

    public MicoCloudEventException(String message, MicoCloudEventImpl<JsonNode> sourceEvent) {
        super(message);
        this.sourceEvent = sourceEvent;
    }

    public MicoCloudEventException(String message, Throwable source, MicoCloudEventImpl<JsonNode> sourceEvent) {
        super(message, source);
        this.sourceEvent = sourceEvent;
    }

    public MicoCloudEventException(Throwable source, MicoCloudEventImpl<JsonNode> sourceEvent) {
        super(source);
        this.sourceEvent = sourceEvent;
    }

    /**
     * Serialize this exception into an error CloudEvent.
     * <p>
     * The returned CloudEvent has an empty source that has to be set!
     *
     * @return the error CloudEvent
     */
    public MicoCloudEventImpl<JsonNode> getErrorEvent() {
        MicoCloudEventImpl<JsonNode> error = new MicoCloudEventImpl<>();
        error.setRandomId();
        error.setIsErrorMessage(true);
        error.setErrorMessage(this.getMessage());
        error.setErrorTrace(this.getStackTraceAsString());
        if (this.sourceEvent != null) {
            try {
                // Set 'created from' to the source id
                String sourceId = this.sourceEvent.getId();
                if (!StringUtils.isEmpty(sourceId)) {
                    error.setCreatedFrom(sourceId);
                }
                // Add the correlation id of the source event
                Optional<String> correlationId = this.sourceEvent.getCorrelationId();
                if (correlationId.isPresent()) {
                    error.setCorrelationId(correlationId.orElse(null));
                }
                // Add the data of the source event
                JsonNode data = Json.MAPPER.convertValue(this.sourceEvent, JsonNode.class);
                error.setData(data);
            } catch (Exception e) {
                log.error("Could not construct a CloudEvent out of this error.", e);
            }
        }
        return error;
    }

    private String getStackTraceAsString() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        this.printStackTrace(pw);
        return sw.toString();
    }
}
