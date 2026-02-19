package io.softwarestrategies.scheduledevent.exception;

/**
 * Exception thrown when a scheduled event is not found.
 */
public class EventNotFoundException extends ScheduledEventException {

	public EventNotFoundException(String identifier) {
		super("Event not found: " + identifier, "EVENT_NOT_FOUND");
	}
}