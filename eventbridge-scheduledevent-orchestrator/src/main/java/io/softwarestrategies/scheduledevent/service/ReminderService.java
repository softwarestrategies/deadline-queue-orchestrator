package io.softwarestrategies.scheduledevent.service;

import org.springframework.stereotype.Service;

import io.softwarestrategies.scheduledevent.data.dto.SendReminderRequest;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ReminderService {

	public void sendReminder(SendReminderRequest sendReminderRequest) {
		log.info("Processing reminder event ... {}", sendReminderRequest);
	}
}
