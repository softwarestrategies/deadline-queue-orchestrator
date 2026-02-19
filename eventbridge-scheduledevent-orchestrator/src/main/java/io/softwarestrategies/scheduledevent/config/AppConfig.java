package io.softwarestrategies.scheduledevent.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

@Configuration
public class AppConfig {

	@Bean
	public ObjectMapper objectMapper() {

		// Create a new mapper
		ObjectMapper mapper = new ObjectMapper();

		// 🚨 IMPORTANT: Manually register the JavaTimeModule
		mapper.registerModule(new JavaTimeModule());

		// Optional: Ensure dates are written as ISO 8601 strings, not long arrays
		// Note: Spring Boot usually handles this, but it's good practice to ensure.
		// mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

		return mapper;
	}
}
