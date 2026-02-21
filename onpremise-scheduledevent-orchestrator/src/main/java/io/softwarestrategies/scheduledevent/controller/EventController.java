package io.softwarestrategies.scheduledevent.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/v1/test-events")
@RequiredArgsConstructor
public class EventController {

	/**
	 * Process Genesys-generated events pushed to the backend via AWS EventBridge, AWS Lambda and Orchestration Service
	 *
	 * @param eventAsMap The event that occurred as Json
	 */
	@PostMapping
	public ResponseEntity<Void> postEvent(@RequestBody Map<String, Object> eventAsMap) {
		return ResponseEntity.ok().build();
	}
}