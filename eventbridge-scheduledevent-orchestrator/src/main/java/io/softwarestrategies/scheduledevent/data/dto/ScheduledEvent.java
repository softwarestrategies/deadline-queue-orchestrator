package io.softwarestrategies.scheduledevent.data.dto;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.annotation.JsonInclude;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonInclude(NON_NULL)
public class ScheduledEvent {

	String id;
	String topic;
	String source;
	String destinationUrl;
	String env;
	ChronoUnit delayUnits;
	Long delayUnitAmount;
	String data;
	LocalDateTime created;

	Boolean supportRetry;		// true;
	Integer tryCount;			// 1;
	Long retryInitialInterval;	// 1000L;
	Integer retryMaxAttempts;	// 3;
	Double retryMultiplier;		// 2.0;
	Double retryRandomFactor;	// 0.5;
}