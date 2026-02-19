package io.softwarestrategies.scheduledevent.config;

import io.softwarestrategies.scheduledevent.dto.KafkaEventMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration optimized for high-throughput event ingestion.
 */
@Configuration
@EnableKafka
@Slf4j
public class KafkaConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${spring.kafka.consumer.group-id}")
	private String consumerGroupId;

	@Value("${app.kafka.topics.ingestion}")
	private String ingestionTopic;

	@Value("${app.kafka.topics.dlq}")
	private String dlqTopic;

	@Value("${app.kafka.topics.execution-http}")
	private String executionHttpTopic;

	@Value("${app.kafka.topics.execution-kafka}")
	private String executionKafkaTopic;

	@Value("${app.kafka.partitions:24}")
	private int partitions;

	@Value("${app.kafka.replication-factor:3}")
	private short replicationFactor;

	@Value("${spring.kafka.consumer.max-poll-records:500}")
	private int maxPollRecords;

	@Value("${spring.kafka.listener.concurrency:10}")
	private int concurrency;

	@Bean
	public KafkaAdmin kafkaAdmin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		return new KafkaAdmin(configs);
	}

	// ==================== Topic Creation ====================

	@Bean
	public NewTopic ingestionTopic() {
		return TopicBuilder.name(ingestionTopic)
				.partitions(partitions)
				.replicas(replicationFactor)
				.config("retention.ms", String.valueOf(7 * 24 * 60 * 60 * 1000L)) // 7 days
				.config("cleanup.policy", "delete")
				.config("min.insync.replicas", "2")
				.build();
	}

	@Bean
	public NewTopic dlqTopic() {
		return TopicBuilder.name(dlqTopic)
				.partitions(partitions / 2)
				.replicas(replicationFactor)
				.config("retention.ms", String.valueOf(30L * 24 * 60 * 60 * 1000)) // 30 days
				.build();
	}

	@Bean
	public NewTopic executionHttpTopic() {
		return TopicBuilder.name(executionHttpTopic)
				.partitions(partitions)
				.replicas(replicationFactor)
				.config("retention.ms", String.valueOf(24 * 60 * 60 * 1000L)) // 1 day
				.build();
	}

	@Bean
	public NewTopic executionKafkaTopic() {
		return TopicBuilder.name(executionKafkaTopic)
				.partitions(partitions)
				.replicas(replicationFactor)
				.config("retention.ms", String.valueOf(24 * 60 * 60 * 1000L)) // 1 day
				.build();
	}

	// ==================== Producer Configuration ====================

	@Bean
	public ProducerFactory<String, Object> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

		// High-throughput settings
		configProps.put(ProducerConfig.ACKS_CONFIG, "all");
		configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
		configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768); // 32KB
		configProps.put(ProducerConfig.LINGER_MS_CONFIG, 10); // Wait up to 10ms to batch
		configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L); // 64MB
		configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
		configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<String, Object> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	// ==================== Consumer Configuration ====================

	@Bean
	public ConsumerFactory<String, KafkaEventMessage> consumerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

		// Configure JSON deserializer
		props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "io.softwarestrategies.scheduledevent.dto");
		props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, KafkaEventMessage.class.getName());
		props.put(JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS, false);

		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, KafkaEventMessage> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, KafkaEventMessage> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(concurrency);
		factory.setBatchListener(true); // Enable batch listening
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

		// Configure error handling with DLQ
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(
				(record, exception) -> {
					log.error("Error processing Kafka message, sending to DLQ. Key: {}, Error: {}",
							record.key(), exception.getMessage());
				},
				new FixedBackOff(1000L, 3) // 3 retries with 1 second delay
		);
		factory.setCommonErrorHandler(errorHandler);

		return factory;
	}

	// ==================== String Consumer Configuration ====================

	@Bean
	public ConsumerFactory<String, String> stringConsumerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId + "-string");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean("stringKafkaListenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, String> stringKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(stringConsumerFactory());
		factory.setConcurrency(concurrency / 2);
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		return factory;
	}
}