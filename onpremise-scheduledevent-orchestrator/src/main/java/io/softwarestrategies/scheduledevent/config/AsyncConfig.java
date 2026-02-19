package io.softwarestrategies.scheduledevent.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * Configuration for async processing and virtual threads.
 */
@Configuration
@EnableAsync
public class AsyncConfig {

	@Value("${app.scheduler.executor-threads:20}")
	private int executorThreads;

	/**
	 * Task executor for async event processing.
	 * Uses virtual threads for optimal performance with I/O-bound tasks.
	 */
	@Bean(name = "eventProcessingExecutor")
	public Executor eventProcessingExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(executorThreads);
		executor.setMaxPoolSize(executorThreads * 2);
		executor.setQueueCapacity(1000);
		executor.setThreadNamePrefix("event-processor-");
		executor.setRejectedExecutionHandler((r, e) -> {
			// Log and potentially queue to Kafka DLQ
			throw new RuntimeException("Task rejected, executor queue is full");
		});
		executor.setVirtualThreads(true); // Enable virtual threads
		executor.initialize();
		return executor;
	}

	/**
	 * Task executor for scheduled tasks.
	 */
	@Bean(name = "schedulerExecutor")
	public Executor schedulerExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(5);
		executor.setMaxPoolSize(10);
		executor.setQueueCapacity(100);
		executor.setThreadNamePrefix("scheduler-");
		executor.setVirtualThreads(true);
		executor.initialize();
		return executor;
	}
}