package com.company.kafka.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.Instant;
import java.util.List;

/**
 * Entity class for storing movement data
 * Represents both real-time and historical movement records
 */
@Entity
@Table(name = "movement_data", indexes = {
    @Index(name = "idx_source_destination", columnList = "sourceLocation,destinationLocation"),
    @Index(name = "idx_timestamp", columnList = "timestamp"),
    @Index(name = "idx_destination", columnList = "destination"),
    @Index(name = "idx_historical", columnList = "isHistorical"),
    @Index(name = "idx_cutover", columnList = "processedDuringCutover")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MovementData {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  /**
   * Destination location from the original message
   */
  @Column(name = "destination", nullable = false, length = 50)
  private String destination;

  /**
   * List of servicing nodes (distribution centers serving this destination)
   */
  @ElementCollection
  @CollectionTable(name = "movement_servicing_nodes", joinColumns = @JoinColumn(name = "movement_id"))
  @Column(name = "servicing_node", length = 50)
  private List<String> servicingNodes;

  /**
   * Source location from the original message
   */
  @Column(name = "sourceLocation", nullable = false, length = 50)
  private String sourceLocation;

  /**
   * Destination location from the original message
   */
  @Column(name = "destinationLocation", nullable = false, length = 50)
  private String destinationLocation;

  /**
   * Status of the movement (PLANNED, IN_TRANSIT, COMPLETED, etc.)
   */
  @Column(name = "status", length = 20)
  private String status;

  /**
   * Timestamp when the movement was originally created/planned
   */
  @Column(name = "timestamp", nullable = false)
  private Instant timestamp;

  /**
   * Flag indicating if this is historical data (fetched via REST API)
   */
  @Column(name = "isHistorical", nullable = false)
  @Builder.Default
  private Boolean isHistorical = false;

  /**
   * Flag indicating if this record was processed during cut-over
   */
  @Column(name = "processedDuringCutover", nullable = false)
  @Builder.Default
  private Boolean processedDuringCutover = false;

  /**
   * Movement type (NORMAL, REVERSE) - determined by processing logic
   */
  @Column(name = "movementType", length = 20)
  private String movementType;

  /**
   * Flow direction (DC_TO_STORE, STORE_TO_DC)
   */
  @Column(name = "flowDirection", length = 30)
  private String flowDirection;

  /**
   * Actual origin after processing (may be swapped for reverse movements)
   */
  @Column(name = "actualOrigin", length = 50)
  private String actualOrigin;

  /**
   * Actual destination after processing (may be swapped for reverse movements)
   */
  @Column(name = "actualDestination", length = 50)
  private String actualDestination;

  /**
   * Validation status
   */
  @Column(name = "validationStatus", length = 20)
  private String validationStatus;

  /**
   * Validation error message if validation failed
   */
  @Column(name = "validationError", length = 500)
  private String validationError;

  /**
   * When this record was processed by our system
   */
  @Column(name = "processedAt", nullable = false)
  @Builder.Default
  private Instant processedAt = Instant.now();

  /**
   * Source of the data (KAFKA, REST_API)
   */
  @Column(name = "dataSource", length = 20)
  @Builder.Default
  private String dataSource = "KAFKA";

  /**
   * Original message ID or correlation ID for tracking
   */
  @Column(name = "correlationId", length = 100)
  private String correlationId;

  /**
   * Additional metadata in JSON format
   */
  @Column(name = "metadata", columnDefinition = "TEXT")
  private String metadata;

  /**
   * Version for optimistic locking
   */
  @Version
  private Long version;

  /**
   * Audit fields
   */
  @Column(name = "createdAt", nullable = false, updatable = false)
  @Builder.Default
  private Instant createdAt = Instant.now();

  @Column(name = "updatedAt")
  private Instant updatedAt;

  @PreUpdate
  protected void onUpdate() {
    updatedAt = Instant.now();
  }

  @PrePersist
  protected void onCreate() {
    if (createdAt == null) {
      createdAt = Instant.now();
    }
    if (processedAt == null) {
      processedAt = Instant.now();
    }
  }

  /**
   * Helper method to check if this is a reverse movement
   */
  public boolean isReverseMovement() {
    return "REVERSE".equals(movementType);
  }

  /**
   * Helper method to check if validation passed
   */
  public boolean isValidationPassed() {
    return "VALID".equals(validationStatus);
  }

  /**
   * Helper method to get display name for movement direction
   */
  public String getMovementDirectionDisplay() {
    if (isReverseMovement()) {
      return String.format("%s → %s (Reverse)", actualOrigin, actualDestination);
    } else {
      return String.format("%s → %s (Normal)", actualOrigin, actualDestination);
    }
  }

  /**
   * Helper method to check if this is cut-over data
   */
  public boolean isCutoverData() {
    return Boolean.TRUE.equals(processedDuringCutover);
  }

  /**
   * Helper method to check if this is historical data
   */
  public boolean isHistoricalData() {
    return Boolean.TRUE.equals(isHistorical);
  }
}