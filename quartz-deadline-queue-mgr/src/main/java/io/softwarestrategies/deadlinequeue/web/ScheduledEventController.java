package io.softwarestrategies.deadlinequeue.web;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.softwarestrategies.deadlinequeue.event.ScheduledEvent;
import io.softwarestrategies.deadlinequeue.service.ScheduledEventService;
import lombok.AllArgsConstructor;

@RestController
@AllArgsConstructor
public class ScheduledEventController {

	private final ScheduledEventService scheduledEventService;

	@PostMapping("/api/v1/scheduled-events")
	public ResponseEntity<String> scheduleAnEvent(@RequestBody ScheduledEvent scheduledEvent) {
		scheduledEventService.scheduleEvent(scheduledEvent);
		return ResponseEntity.ok().build();
	}
}
