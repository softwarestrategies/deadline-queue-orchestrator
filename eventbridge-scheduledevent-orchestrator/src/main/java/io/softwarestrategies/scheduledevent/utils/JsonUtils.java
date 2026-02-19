package io.softwarestrategies.scheduledevent.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonUtils {

	public static String toJson(ObjectMapper objectMapper, Object object, String objectName) {
		try {
			return objectMapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			log.error("Unable to serialize {} to JSON", objectName, e);
			throw new RuntimeException("Unable to serialize {%s} to JSON".formatted(objectName));
		}
	}
}
