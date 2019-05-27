package io.github.ust.mico.msgfunctionrouter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class MsgFunctionRouterApplication {

	public static void main(String[] args) {
		SpringApplication.run(MsgFunctionRouterApplication.class, args);
	}

	/**
	 * @param builder
	 * @return
	 * @see <a href="https://gist.github.com/RealDeanZhao/38821bc1efeb7e2a9bcd554cc06cdf96">RealDeanZhao/autowire-resttemplate.md</a>
	 */
	@Bean
	public RestTemplate restTemplate(RestTemplateBuilder builder) {
		return builder.build();
	}
}
