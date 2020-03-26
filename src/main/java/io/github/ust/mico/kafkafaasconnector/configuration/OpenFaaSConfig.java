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

package io.github.ust.mico.kafkafaasconnector.configuration;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.net.MalformedURLException;
import java.net.URL;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * Configuration for the OpenFaaS connection.
 */
@Slf4j
@Component
@Setter
@Getter
@ConfigurationProperties("openfaas")
public class OpenFaaSConfig {

    /**
     * The URL of the OpenFaaS gateway.
     */
    @NotBlank
    private String gateway;

    /**
     * The OpenFaaS function name.
     */
    @NotBlank
    private String functionName;

    /**
     * Whether to skip calling the OpenFaaS function.
     *
     * This can be used for debugging or testing the generic logic.
     */
    @NotNull
    private boolean skipFunctionCall = false;

    private String patternConfig;

    /**
     * Get the url for the configured function using gateway and function.
     *
     * @return the constructed url
     * @throws MalformedURLException if the url is invalid
     */
    public URL getFunctionUrl() throws MalformedURLException {
        try {
            URL gatewayUrl = new URL(this.getGateway());
            URL functionUrl = new URL(gatewayUrl.getProtocol(), gatewayUrl.getHost(), gatewayUrl.getPort(),
                    gatewayUrl.getFile() + "/function/" + this.getFunctionName(), null);
            return functionUrl;
        } catch (MalformedURLException e) {
            log.error("Invalid URL to OpenFaaS gateway ({}) or function name ({}). Caused by: {}",
                    this.getGateway(), this.getFunctionName(), e.getMessage());
            throw e;
        }
    }
}
