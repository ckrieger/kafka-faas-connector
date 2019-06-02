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
