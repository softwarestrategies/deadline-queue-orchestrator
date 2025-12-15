package io.softwarestrategies.deadlinequeue.web;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.softwarestrategies.deadlinequeue.data.dto.SendReminderRequest;
import io.softwarestrategies.deadlinequeue.service.ReminderService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/v1/reminders")
@AllArgsConstructor
@Slf4j
public class ReminderController {

	private final ReminderService reminderService;

	@PostMapping
	public void scheduleReminder(@RequestBody SendReminderRequest sendReminderRequest) {
		log.info("Scheduling reminder: {}", sendReminderRequest);
		reminderService.sendReminder(sendReminderRequest);
	}
}
