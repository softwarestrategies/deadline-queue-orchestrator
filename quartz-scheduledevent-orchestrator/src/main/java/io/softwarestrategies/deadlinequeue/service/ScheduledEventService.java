package io.softwarestrategies.deadlinequeue.service;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.softwarestrategies.deadlinequeue.event.ScheduledEvent;
import io.softwarestrategies.deadlinequeue.event.ScheduledEventJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class ScheduledEventService {

	private final Scheduler scheduler;
	private final ObjectMapper objectMapper;

	/**
	 * Schedules a one-time job to execute an API push after a specified delay.

	 * @param scheduledEvent The JSON body of the event.
	 */
	public void scheduleEvent(ScheduledEvent scheduledEvent) {

		try {
			String jobName = "event-" + UUID.randomUUID();
			String groupName = "scheduled-events";

			JobDetail jobDetail = JobBuilder.newJob(ScheduledEventJob.class)
					.withIdentity(jobName, groupName)
					.usingJobData("scheduledEvent", objectMapper.writeValueAsString(scheduledEvent))
					.storeDurably()
					.build();

			//Date startTime = new Date(System.currentTimeMillis() + scheduledEvent.getDelayInMs());
			Date startTime = dateFromNow(scheduledEvent.getDelayUnits(), scheduledEvent.getDelayUnitAmount());

			Trigger trigger = TriggerBuilder.newTrigger()
					.withIdentity("trigger-" + jobName, groupName)
					.startAt(startTime)
					.withSchedule(SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionFireNow())
					.build();

			scheduler.scheduleJob(jobDetail, trigger);

			log.info("Scheduled Job %s to fire at %s".formatted(jobName, startTime));

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static Date dateFromNow(ChronoUnit unit, long amount) {
		Objects.requireNonNull(unit, "unit must not be null");

		// ZonedDateTime is timezone-aware; using systemDefault() == "current timezone"
		ZonedDateTime target = ZonedDateTime.now(ZoneId.systemDefault()).plus(amount, unit);

		return Date.from(target.toInstant());
	}
}
