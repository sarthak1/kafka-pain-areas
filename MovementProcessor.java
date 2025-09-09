package com.company.kafka.movement;

import com.company.kafka.service.LocationService;
import com.company.kafka.service.MovementValidationService;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Service to handle movement type detection and processing
 * Addresses Pain Area 1: Reverse Movement Handling
 */
@Slf4j
@Service
public class MovementProcessor {

  @Autowired
  private LocationService locationService;

  @Autowired
  private MovementValidationService validationService;

  /**
   * Process incoming Kafka message and determine movement type
   */
  public ProcessedMovement processMovement(KafkaMovementMessage message) {
    log.debug("Processing movement message: {}", message);

    MovementType detectedType = detectMovementType(message);
    ProcessedMovement processed = createProcessedMovement(message, detectedType);

    // Validate the processed movement
    ValidationResult validation = validationService.validate(processed);
    processed.setValidation(validation);

    log.info("Processed movement - Type: {}, Origin: {}, Destination: {}, Valid: {}",
        detectedType, processed.getActualOrigin(), processed.getActualDestination(),
        validation.isValid());

    return processed;
  }

  /**
   * Detect if this is a normal or reverse movement
   * Strategy 1: Business rule based on location types
   */
  private MovementType detectMovementType(KafkaMovementMessage message) {
    String sourceLocation = message.getSourceLocation();
    String destinationLocation = message.getDestinationLocation();

    LocationService.LocationType sourceType = locationService.getLocationType(sourceLocation);
    LocationService.LocationType destinationType = locationService.getLocationType(destinationLocation);

    log.debug("Source {} is type {}, Destination {} is type {}",
        sourceLocation, sourceType, destinationLocation, destinationType);

    // Business Rule: If source is STORE and destination is DC, it's reverse
    if (sourceType == LocationService.LocationType.STORE && destinationType == LocationService.LocationType.DC) {
      log.info("Detected REVERSE movement from store {} to DC {}",
          sourceLocation, destinationLocation);
      return MovementType.REVERSE;
    }

    // Strategy 2: Check if destination is in servicing nodes (fallback)
    if (message.getServicingNodes().contains(message.getDestination())) {
      log.info("Detected REVERSE movement via servicing nodes check");
      return MovementType.REVERSE;
    }

    return MovementType.NORMAL;
  }

  /**
   * Create processed movement with correct origin/destination mapping
   */
  private ProcessedMovement createProcessedMovement(KafkaMovementMessage message,
      MovementType type) {
    ProcessedMovement.ProcessedMovementBuilder builder = ProcessedMovement.builder()
        .originalMessage(message)
        .movementType(type)
        .processedAt(java.time.Instant.now());

    if (type == MovementType.REVERSE) {
      // In reverse movement, swap the semantic meaning
      builder.actualOrigin(message.getDestinationLocation())
          .actualDestination(message.getSourceLocation())
          .flowDirection("STORE_TO_DC");
    } else {
      // Normal movement, use as-is
      builder.actualOrigin(message.getSourceLocation())
          .actualDestination(message.getDestinationLocation())
          .flowDirection("DC_TO_STORE");
    }

    return builder.build();
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ProcessedMovement {
    private KafkaMovementMessage originalMessage;
    private MovementType movementType;
    private String actualOrigin;
    private String actualDestination;
    private String flowDirection;
    private ValidationResult validation;
    private java.time.Instant processedAt;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class KafkaMovementMessage {
    private String destination;
    private List<String> servicingNodes;
    private String sourceLocation;
    private String destinationLocation;
    private String status; // PLANNED, IN_TRANSIT, etc.
    private java.time.Instant timestamp;
  }

  public enum MovementType {
    NORMAL, // DC to Store
    REVERSE // Store to DC
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ValidationResult {
    private boolean valid;
    private String errorMessage;
    private String validationCode;
  }
}