package io.github.ust.mico.kafkafaasconnector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class KafkaFaaSConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaFaaSConnectorApplication.class, args);
    }

    /**
     * @see <a href="https://gist.github.com/RealDeanZhao/38821bc1efeb7e2a9bcd554cc06cdf96">RealDeanZhao/autowire-resttemplate.md</a>
     */
    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder.build();
    }
}
