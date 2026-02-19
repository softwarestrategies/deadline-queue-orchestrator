package io.softwarestrategies.scheduledevent.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * DTO for batch scheduled event creation requests.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BatchScheduledEventRequest {

	@NotEmpty(message = "Events list cannot be empty")
	@Size(max = 1000, message = "Batch size cannot exceed 1000 events")
	@Valid
	private List<ScheduledEventRequest> events;
}