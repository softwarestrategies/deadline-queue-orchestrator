package io.softwarestrategies.scheduledevent.domain;

/**
 * Defines the delivery mechanism for scheduled events.
 */
public enum DeliveryType {
	/**
	 * Deliver the event payload via HTTP POST to an external endpoint
	 */
	HTTP,

	/**
	 * Deliver the event payload to a Kafka topic
	 */
	KAFKA
}
