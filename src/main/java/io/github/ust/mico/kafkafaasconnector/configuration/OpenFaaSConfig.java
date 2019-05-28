package io.github.ust.mico.kafkafaasconnector.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotBlank;

/**
 * Configuration for the OpenFaaS connection.
 */
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
}
