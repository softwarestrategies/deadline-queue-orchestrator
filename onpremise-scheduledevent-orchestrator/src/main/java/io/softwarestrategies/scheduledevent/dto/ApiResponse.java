package io.softwarestrategies.scheduledevent.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * Standard API response wrapper.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResponse<T> {

	private boolean success;

	private T data;

	private String message;

	@JsonProperty("error_code")
	private String errorCode;

	private List<String> errors;

	@Builder.Default
	private Instant timestamp = Instant.now();

	public static <T> ApiResponse<T> success(T data) {
		return ApiResponse.<T>builder()
				.success(true)
				.data(data)
				.build();
	}

	public static <T> ApiResponse<T> success(T data, String message) {
		return ApiResponse.<T>builder()
				.success(true)
				.data(data)
				.message(message)
				.build();
	}

	public static <T> ApiResponse<T> error(String message, String errorCode) {
		return ApiResponse.<T>builder()
				.success(false)
				.message(message)
				.errorCode(errorCode)
				.build();
	}

	public static <T> ApiResponse<T> error(String message, String errorCode, List<String> errors) {
		return ApiResponse.<T>builder()
				.success(false)
				.message(message)
				.errorCode(errorCode)
				.errors(errors)
				.build();
	}
}