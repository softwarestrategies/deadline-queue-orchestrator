package io.softwarestrategies.scheduledevent.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * WebClient configuration for high-throughput HTTP event delivery.
 */
@Configuration
public class WebClientConfig {

	@Value("${app.http-client.connect-timeout-ms:5000}")
	private int connectTimeoutMs;

	@Value("${app.http-client.read-timeout-ms:30000}")
	private int readTimeoutMs;

	@Value("${app.http-client.max-connections:200}")
	private int maxConnections;

	@Value("${app.http-client.max-connections-per-route:50}")
	private int maxConnectionsPerRoute;

	@Bean
	public WebClient webClient() {
		// Configure connection pooling
		ConnectionProvider connectionProvider = ConnectionProvider.builder("event-delivery-pool")
				.maxConnections(maxConnections)
				.maxIdleTime(Duration.ofSeconds(30))
				.maxLifeTime(Duration.ofMinutes(5))
				.pendingAcquireTimeout(Duration.ofSeconds(45))
				.pendingAcquireMaxCount(-1) // No limit on pending
				.evictInBackground(Duration.ofSeconds(120))
				.build();

		// Configure HTTP client with timeouts
		HttpClient httpClient = HttpClient.create(connectionProvider)
				.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
				.responseTimeout(Duration.ofMillis(readTimeoutMs))
				.doOnConnected(conn -> conn
						.addHandlerLast(new ReadTimeoutHandler(readTimeoutMs, TimeUnit.MILLISECONDS))
						.addHandlerLast(new WriteTimeoutHandler(readTimeoutMs, TimeUnit.MILLISECONDS)));

		// Configure exchange strategies for large payloads
		ExchangeStrategies exchangeStrategies = ExchangeStrategies.builder()
				.codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024)) // 16MB
				.build();

		return WebClient.builder()
				.clientConnector(new ReactorClientHttpConnector(httpClient))
				.exchangeStrategies(exchangeStrategies)
				.build();
	}
}