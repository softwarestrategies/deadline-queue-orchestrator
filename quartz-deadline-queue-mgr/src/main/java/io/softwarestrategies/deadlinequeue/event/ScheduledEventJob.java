package io.softwarestrategies.deadlinequeue.event;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@RequiredArgsConstructor
public class ScheduledEventJob implements Job {

	private final ObjectMapper objectMapper;

	/**
	 * Execute the scheduled event.
	 *
	 * @param context The job execution context.
	 */
	@Override
	public void execute(JobExecutionContext context) {
		try {
			JobDataMap dataMap = context.getMergedJobDataMap();

			ScheduledEvent scheduledEvent = objectMapper.readValue(dataMap.getString("scheduledEvent"),ScheduledEvent.class);
			log.info("Processing event: {}", scheduledEvent);

			processEvent(scheduledEvent);

		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Process the scheduled event.
	 *
	 * @param scheduledEvent The scheduled event.
	 */
	private void processEvent(ScheduledEvent scheduledEvent) {
		String destinationUrl = scheduledEvent.getDestinationUrl();
		String requestBodyJson = scheduledEvent.getData();

		if (destinationUrl == null || destinationUrl.isBlank()) {
			throw new IllegalArgumentException("scheduledEvent.destinationUrl must be provided");
		}
		if (requestBodyJson == null || requestBodyJson.isBlank()) {
			throw new IllegalArgumentException("scheduledEvent.data must be provided");
		}

		RestClient restClient = RestClient.create();

		restClient.post()
				.uri(destinationUrl)
				.contentType(MediaType.APPLICATION_JSON)
				.body(requestBodyJson)
				.retrieve()
				.toBodilessEntity();
	}
}