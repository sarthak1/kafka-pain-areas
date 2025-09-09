package com.company.kafka.service;

import com.company.kafka.movement.MovementProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Service to validate movement data
 * Performs business rule validation on processed movements
 */
@Slf4j
@Service
public class MovementValidationService {

  @Value("${movement.validation.strict-mode:false}")
  private boolean strictMode;

  @Value("${movement.validation.allow-unknown-locations:true}")
  private boolean allowUnknownLocations;

  @Value("${movement.validation.max-timestamp-future-hours:24}")
  private int maxTimestampFutureHours;

  @Value("${movement.validation.max-timestamp-past-days:365}")
  private int maxTimestampPastDays;

  /**
   * Validate a processed movement
   */
  public MovementProcessor.ValidationResult validate(MovementProcessor.ProcessedMovement movement) {
    if (movement == null) {
      return createErrorResult("MOVEMENT_NULL", "Movement cannot be null");
    }

    List<String> validationErrors = new ArrayList<>();

    // Validate basic fields
    validateBasicFields(movement, validationErrors);

    // Validate locations
    validateLocations(movement, validationErrors);

    // Validate timestamp
    validateTimestamp(movement, validationErrors);

    // Validate movement direction consistency
    validateMovementDirection(movement, validationErrors);

    // Validate servicing nodes
    validateServicingNodes(movement, validationErrors);

    // Build result
    if (validationErrors.isEmpty()) {
      return createSuccessResult();
    } else {
      String combinedErrors = String.join("; ", validationErrors);
      return createErrorResult("VALIDATION_FAILED", combinedErrors);
    }
  }

  /**
   * Validate basic required fields
   */
  private void validateBasicFields(MovementProcessor.ProcessedMovement movement, List<String> errors) {
    MovementProcessor.KafkaMovementMessage originalMessage = movement.getOriginalMessage();

    if (originalMessage == null) {
      errors.add("Original message is null");
      return;
    }

    if (isNullOrEmpty(originalMessage.getDestination())) {
      errors.add("Destination is required");
    }

    if (isNullOrEmpty(originalMessage.getSourceLocation())) {
      errors.add("Source location is required");
    }

    if (isNullOrEmpty(originalMessage.getDestinationLocation())) {
      errors.add("Destination location is required");
    }

    if (isNullOrEmpty(movement.getActualOrigin())) {
      errors.add("Actual origin is required");
    }

    if (isNullOrEmpty(movement.getActualDestination())) {
      errors.add("Actual destination is required");
    }

    if (movement.getMovementType() == null) {
      errors.add("Movement type is required");
    }
  }

  /**
   * Validate location data
   */
  private void validateLocations(MovementProcessor.ProcessedMovement movement, List<String> errors) {
    String actualOrigin = movement.getActualOrigin();
    String actualDestination = movement.getActualDestination();

    // Check for same origin and destination
    if (actualOrigin != null && actualOrigin.equals(actualDestination)) {
      errors.add("Origin and destination cannot be the same: " + actualOrigin);
    }

    // Validate location format (basic pattern check)
    if (!isValidLocationFormat(actualOrigin)) {
      errors.add("Invalid origin location format: " + actualOrigin);
    }

    if (!isValidLocationFormat(actualDestination)) {
      errors.add("Invalid destination location format: " + actualDestination);
    }

    // In strict mode, validate location consistency with movement type
    if (strictMode) {
      validateLocationConsistency(movement, errors);
    }
  }

  /**
   * Validate timestamp
   */
  private void validateTimestamp(MovementProcessor.ProcessedMovement movement, List<String> errors) {
    Instant timestamp = movement.getOriginalMessage().getTimestamp();

    if (timestamp == null) {
      errors.add("Timestamp is required");
      return;
    }

    Instant now = Instant.now();

    // Check if timestamp is too far in the future
    Instant maxFuture = now.plusSeconds(maxTimestampFutureHours * 3600L);
    if (timestamp.isAfter(maxFuture)) {
      errors.add("Timestamp is too far in the future: " + timestamp);
    }

    // Check if timestamp is too far in the past
    Instant minPast = now.minusSeconds(maxTimestampPastDays * 24L * 3600L);
    if (timestamp.isBefore(minPast)) {
      errors.add("Timestamp is too far in the past: " + timestamp);
    }
  }

  /**
   * Validate movement direction consistency
   */
  private void validateMovementDirection(MovementProcessor.ProcessedMovement movement, List<String> errors) {
    MovementProcessor.MovementType movementType = movement.getMovementType();
    String flowDirection = movement.getFlowDirection();

    if (movementType == MovementProcessor.MovementType.NORMAL) {
      if (!"DC_TO_STORE".equals(flowDirection)) {
        errors.add("Normal movement should have DC_TO_STORE flow direction, but found: " + flowDirection);
      }
    } else if (movementType == MovementProcessor.MovementType.REVERSE) {
      if (!"STORE_TO_DC".equals(flowDirection)) {
        errors.add("Reverse movement should have STORE_TO_DC flow direction, but found: " + flowDirection);
      }
    }
  }

  /**
   * Validate servicing nodes
   */
  private void validateServicingNodes(MovementProcessor.ProcessedMovement movement, List<String> errors) {
    List<String> servicingNodes = movement.getOriginalMessage().getServicingNodes();

    if (servicingNodes == null || servicingNodes.isEmpty()) {
      errors.add("Servicing nodes cannot be empty");
      return;
    }

    // Check for duplicate servicing nodes
    long distinctCount = servicingNodes.stream().distinct().count();
    if (distinctCount != servicingNodes.size()) {
      errors.add("Duplicate servicing nodes found");
    }

    // Validate each servicing node format
    for (String node : servicingNodes) {
      if (!isValidLocationFormat(node)) {
        errors.add("Invalid servicing node format: " + node);
        break; // Don't spam with multiple similar errors
      }
    }

    // For reverse movements, destination should be in servicing nodes
    if (movement.getMovementType() == MovementProcessor.MovementType.REVERSE) {
      String destination = movement.getOriginalMessage().getDestination();
      if (!servicingNodes.contains(destination)) {
        errors.add("For reverse movements, destination should be in servicing nodes");
      }
    }
  }

  /**
   * Validate location consistency with movement type (strict mode)
   */
  private void validateLocationConsistency(MovementProcessor.ProcessedMovement movement, List<String> errors) {
    // This would require integration with LocationService to check location types
    // For now, we'll do basic format validation

    String actualOrigin = movement.getActualOrigin();
    String actualDestination = movement.getActualDestination();
    MovementProcessor.MovementType movementType = movement.getMovementType();

    // Basic heuristic: stores typically start with '2', DCs with '9' or '1'
    boolean originLooksLikeStore = actualOrigin.startsWith("2");
    boolean destinationLooksLikeStore = actualDestination.startsWith("2");

    if (movementType == MovementProcessor.MovementType.NORMAL) {
      // Normal: DC to Store
      if (originLooksLikeStore && destinationLooksLikeStore) {
        errors.add("Normal movement should be DC to Store, but both look like stores");
      }
    } else if (movementType == MovementProcessor.MovementType.REVERSE) {
      // Reverse: Store to DC
      if (!originLooksLikeStore && destinationLooksLikeStore) {
        errors.add("Reverse movement should be Store to DC, but origin doesn't look like store");
      }
    }
  }

  /**
   * Check if location format is valid (basic pattern)
   */
  private boolean isValidLocationFormat(String location) {
    if (isNullOrEmpty(location)) {
      return false;
    }

    // Basic validation: 3-4 digit numbers
    return location.matches("^\\d{3,4}$");
  }

  /**
   * Helper method to check if string is null or empty
   */
  private boolean isNullOrEmpty(String str) {
    return str == null || str.trim().isEmpty();
  }

  /**
   * Create success validation result
   */
  private MovementProcessor.ValidationResult createSuccessResult() {
    return MovementProcessor.ValidationResult.builder()
        .valid(true)
        .validationCode("VALID")
        .build();
  }

  /**
   * Create error validation result
   */
  private MovementProcessor.ValidationResult createErrorResult(String code, String message) {
    return MovementProcessor.ValidationResult.builder()
        .valid(false)
        .validationCode(code)
        .errorMessage(message)
        .build();
  }

  /**
   * Validate multiple movements in batch
   */
  public List<MovementProcessor.ValidationResult> validateBatch(
      List<MovementProcessor.ProcessedMovement> movements) {

    if (movements == null || movements.isEmpty()) {
      return new ArrayList<>();
    }

    return movements.stream()
        .map(this::validate)
        .toList();
  }

  /**
   * Get validation summary for a batch of movements
   */
  public ValidationSummary getValidationSummary(List<MovementProcessor.ValidationResult> results) {
    if (results == null || results.isEmpty()) {
      return ValidationSummary.builder()
          .totalValidated(0)
          .validCount(0)
          .invalidCount(0)
          .validationRate(0.0)
          .build();
    }

    long validCount = results.stream().mapToLong(r -> r.isValid() ? 1 : 0).sum();
    long invalidCount = results.size() - validCount;
    double validationRate = (double) validCount / results.size() * 100;

    return ValidationSummary.builder()
        .totalValidated(results.size())
        .validCount((int) validCount)
        .invalidCount((int) invalidCount)
        .validationRate(validationRate)
        .build();
  }

  /**
   * Data class for validation summary
   */
  @lombok.Data
  @lombok.Builder
  @lombok.NoArgsConstructor
  @lombok.AllArgsConstructor
  public static class ValidationSummary {
    private int totalValidated;
    private int validCount;
    private int invalidCount;
    private double validationRate;
  }
}