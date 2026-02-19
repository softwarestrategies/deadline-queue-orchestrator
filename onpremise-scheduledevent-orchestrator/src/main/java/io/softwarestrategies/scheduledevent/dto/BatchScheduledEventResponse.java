package io.softwarestrategies.scheduledevent.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response DTO for batch scheduled event submissions.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BatchScheduledEventResponse {

	@JsonProperty("total_submitted")
	private int totalSubmitted;

	@JsonProperty("total_accepted")
	private int totalAccepted;

	@JsonProperty("total_rejected")
	private int totalRejected;

	@JsonProperty("accepted_message_ids")
	private List<String> acceptedMessageIds;

	@JsonProperty("rejected_events")
	private List<RejectedEvent> rejectedEvents;

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@Builder
	public static class RejectedEvent {

		private int index;

		@JsonProperty("external_job_id")
		private String externalJobId;

		private String reason;
	}
}