package io.softwarestrategies.scheduledevent.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.softwarestrategies.scheduledevent.config.MetricsConfig;
import io.softwarestrategies.scheduledevent.domain.EventStatus;
import io.softwarestrategies.scheduledevent.repository.ScheduledEventRepository;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class MetricsUpdater {

	private final ScheduledEventRepository repository;
	private final MetricsConfig metricsConfig;

	@Scheduled(fixedRate = 5000)  // Every 5 seconds
	public void updateGauges() {
		metricsConfig.updatePendingEventsGauge(
				repository.countByStatus(EventStatus.PENDING));
		metricsConfig.updateProcessingEventsGauge(
				repository.countByStatus(EventStatus.PROCESSING));
	}
}