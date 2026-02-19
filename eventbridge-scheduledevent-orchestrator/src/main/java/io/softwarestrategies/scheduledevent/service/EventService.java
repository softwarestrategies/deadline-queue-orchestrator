package io.softwarestrategies.scheduledevent.service;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import io.softwarestrategies.scheduledevent.data.dto.ScheduledEvent;
import io.softwarestrategies.scheduledevent.utils.JsonUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.scheduler.model.CreateScheduleRequest;
import software.amazon.awssdk.services.scheduler.model.CreateScheduleResponse;
import software.amazon.awssdk.services.scheduler.model.FlexibleTimeWindow;
import software.amazon.awssdk.services.scheduler.model.FlexibleTimeWindowMode;
import software.amazon.awssdk.services.scheduler.model.Target;

import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.ObjectMapper;

@Service
@Slf4j
@RequiredArgsConstructor
public class EventService {

	@Value("${application.event.source}")
	private String EVENT_SOURCE;

	@Value("${application.env}")
	private String ENV;

	@Value("${AWS_REGION}")
	private String AWS_REGION;

	@Value("${AWS_LAMBDA_TARGET_FUNCTION_ARN}")
	private String LAMBDA_TARGET_FUNCTION_ARN;

	@Value("${AWS_EVENTBRIDGE_SCHEDULER_EXECUTION_ROLE_ARN}")
	private String EVENTBRIDGE_SCHEDULER_EXECUTION_ROLE_ARN;

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private final ObjectMapper objectMapper;

	/**
	 * Schedule an event using EventBridge Scheduler
	 *
	 * @param scheduledEvent
	 */
	public void scheduleEvent(ScheduledEvent scheduledEvent) {

		// Configure the delay
		ChronoUnit eventDelayUnits = scheduledEvent.getDelayUnits();
		long eventDelayAmount = scheduledEvent.getDelayUnitAmount();

        String eventScheduledDateTime
				= dateTimeFormatter.withZone(ZoneOffset.UTC).format(Instant.now().plus(eventDelayAmount, eventDelayUnits));

		scheduledEvent.setId(UUID.randomUUID().toString());
		scheduledEvent.setCreated(LocalDateTime.now());

        String scheduledEventRequestAsString = JsonUtils.toJson(objectMapper, scheduledEvent, "ScheduledEvent");
		log.info("ScheduledEvent: {}", scheduledEventRequestAsString);

        // Initialize the Scheduler client
        SchedulerClient schedulerClient = SchedulerClient.builder()
                .region(Region.of(AWS_REGION))
				.httpClientBuilder(ApacheHttpClient.builder()
						.tlsTrustManagersProvider(this::getTrustAllManager))
                .build();

        // Create a unique schedule name
        String scheduleName = "schedule-" + System.currentTimeMillis();

		// Set up the target (Lambda function)
        Target target = Target.builder()
                .arn(LAMBDA_TARGET_FUNCTION_ARN)
                .roleArn(EVENTBRIDGE_SCHEDULER_EXECUTION_ROLE_ARN)
                .input(scheduledEventRequestAsString)
                .build();

        // Configure the schedule to run exactly at the specified time

        FlexibleTimeWindow timeWindow = FlexibleTimeWindow.builder()
                .mode(FlexibleTimeWindowMode.OFF)
                .build();

        // Create the schedule request
        CreateScheduleRequest request = CreateScheduleRequest.builder()
                .name(scheduleName)
                .scheduleExpression("at(" + eventScheduledDateTime + ")") // ISO8601 format
                .target(target)
                .flexibleTimeWindow(timeWindow)
                .build();

        // Create the schedule and return its ARN
        CreateScheduleResponse response = schedulerClient.createSchedule(request);

        // There will be better logging here, but good enough for now
        log.info("Successfully created schedule: " + response.scheduleArn());
        log.info("Event will execute at: " + eventScheduledDateTime);
    }

	/**
	 * WARNING: Only use this for local development/testing!  This bypasses SSL certificate validation.
	 */
	private TrustManager[] getTrustAllManager() {
		return new TrustManager[]{
				new X509TrustManager() {
					public X509Certificate[] getAcceptedIssuers() {
						return new X509Certificate[0];
					}
					public void checkClientTrusted(X509Certificate[] certs, String authType) {}
					public void checkServerTrusted(X509Certificate[] certs, String authType) {}
				}
		};
	}

	/**
	 * Process a previously-scheduled event
	 *
	 * @param scheduledEvent
	 */
	public void processEvent(ScheduledEvent scheduledEvent) {
		log.info("Processing event: {}", scheduledEvent);

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

		log.debug("Posted event data to destinationUrl={}", destinationUrl);
	}
}
