package io.softwarestrategies.scheduledevent.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;

/**
 * RestClient configuration for high-throughput HTTP event delivery.
 * Uses JDK HttpClient with virtual threads for efficient blocking I/O.
 */
@Configuration
public class RestClientConfig {

	@Value("${app.http-client.connect-timeout-ms:5000}")
	private int connectTimeoutMs;

	@Value("${app.http-client.read-timeout-ms:30000}")
	private int readTimeoutMs;

	@Bean
	public RestClient restClient() {
		HttpClient httpClient = HttpClient.newBuilder()
				.connectTimeout(Duration.ofMillis(connectTimeoutMs))
				.build();

		JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
		requestFactory.setReadTimeout(Duration.ofMillis(readTimeoutMs));

		return RestClient.builder()
				.requestFactory(requestFactory)
				.build();
	}
}