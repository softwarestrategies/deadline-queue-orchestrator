package io.softwarestrategies.scheduledevent.exception;

import io.softwarestrategies.scheduledevent.dto.ApiResponse;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Global exception handler for consistent API error responses.
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

	@ExceptionHandler(EventNotFoundException.class)
	public ResponseEntity<ApiResponse<Void>> handleEventNotFoundException(EventNotFoundException ex) {
		log.warn("Event not found: {}", ex.getMessage());
		return ResponseEntity.status(HttpStatus.NOT_FOUND)
				.body(ApiResponse.error(ex.getMessage(), ex.getErrorCode()));
	}

	@ExceptionHandler(ScheduledEventException.class)
	public ResponseEntity<ApiResponse<Void>> handleScheduledEventException(ScheduledEventException ex) {
		log.error("Scheduled event error: {}", ex.getMessage(), ex);
		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
				.body(ApiResponse.error(ex.getMessage(), ex.getErrorCode()));
	}

	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<ApiResponse<Void>> handleValidationException(MethodArgumentNotValidException ex) {
		List<String> errors = ex.getBindingResult().getFieldErrors().stream()
				.map(error -> error.getField() + ": " + error.getDefaultMessage())
				.collect(Collectors.toList());

		log.warn("Validation failed: {}", errors);
		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
				.body(ApiResponse.error("Validation failed", "VALIDATION_ERROR", errors));
	}

	@ExceptionHandler(ConstraintViolationException.class)
	public ResponseEntity<ApiResponse<Void>> handleConstraintViolationException(ConstraintViolationException ex) {
		List<String> errors = ex.getConstraintViolations().stream()
				.map(v -> v.getPropertyPath() + ": " + v.getMessage())
				.collect(Collectors.toList());

		log.warn("Constraint violation: {}", errors);
		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
				.body(ApiResponse.error("Validation failed", "CONSTRAINT_VIOLATION", errors));
	}

	@ExceptionHandler(HttpMessageNotReadableException.class)
	public ResponseEntity<ApiResponse<Void>> handleHttpMessageNotReadableException(HttpMessageNotReadableException ex) {
		log.warn("Invalid request body: {}", ex.getMessage());
		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
				.body(ApiResponse.error("Invalid request body", "INVALID_REQUEST_BODY"));
	}

	@ExceptionHandler(MethodArgumentTypeMismatchException.class)
	public ResponseEntity<ApiResponse<Void>> handleTypeMismatchException(MethodArgumentTypeMismatchException ex) {
		String message = String.format("Invalid value '%s' for parameter '%s'", ex.getValue(), ex.getName());
		log.warn(message);
		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
				.body(ApiResponse.error(message, "TYPE_MISMATCH"));
	}

	@ExceptionHandler(Exception.class)
	public ResponseEntity<ApiResponse<Void>> handleGenericException(Exception ex) {
		log.error("Unexpected error", ex);
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
				.body(ApiResponse.error("An unexpected error occurred", "INTERNAL_ERROR"));
	}
}