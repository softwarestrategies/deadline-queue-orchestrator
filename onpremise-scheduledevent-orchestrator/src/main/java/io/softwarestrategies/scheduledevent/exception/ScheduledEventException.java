package io.softwarestrategies.scheduledevent.exception;

import lombok.Getter;

/**
 * Base exception for scheduled event operations.
 */
@Getter
public class ScheduledEventException extends RuntimeException {

	private final String errorCode;

	public ScheduledEventException(String message, String errorCode) {
		super(message);
		this.errorCode = errorCode;
	}

	public ScheduledEventException(String message, String errorCode, Throwable cause) {
		super(message, cause);
		this.errorCode = errorCode;
	}

}