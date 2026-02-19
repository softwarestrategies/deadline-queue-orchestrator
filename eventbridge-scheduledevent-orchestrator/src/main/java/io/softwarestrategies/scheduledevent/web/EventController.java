package io.softwarestrategies.scheduledevent.web;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.softwarestrategies.scheduledevent.data.dto.ScheduledEvent;
import io.softwarestrategies.scheduledevent.service.EventService;
import lombok.AllArgsConstructor;

@RestController
@AllArgsConstructor
public class EventController {

	private final EventService eventService;

	@PostMapping("/api/v1/scheduled-events")
	public ResponseEntity<String> scheduleAnEvent(@RequestBody ScheduledEvent scheduledEvent) {
		eventService.scheduleEvent(scheduledEvent);
		return ResponseEntity.ok().build();
	}

	@PostMapping("/api/v1/events")
	public ResponseEntity<String> processEvent(@RequestBody ScheduledEvent scheduledEvent) {
		eventService.processEvent(scheduledEvent);
		return ResponseEntity.ok().build();
	}
}
