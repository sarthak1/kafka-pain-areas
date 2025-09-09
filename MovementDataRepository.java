package com.company.kafka.repository;

import com.company.kafka.entity.MovementData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Repository interface for MovementData entity
 * Handles persistence operations for movement data including historical records
 */
@Repository
public interface MovementDataRepository extends JpaRepository<MovementData, Long> {

  /**
   * Find movements by source and destination locations
   */
  List<MovementData> findBySourceLocationAndDestinationLocation(
      String sourceLocation, String destinationLocation);

  /**
   * Find movements processed during cut-over
   */
  List<MovementData> findByProcessedDuringCutoverTrue();

  /**
   * Find historical movements
   */
  List<MovementData> findByIsHistoricalTrue();

  /**
   * Find movements by status
   */
  List<MovementData> findByStatus(String status);

  /**
   * Find movements within a date range
   */
  @Query("SELECT m FROM MovementData m WHERE m.timestamp BETWEEN :startTime AND :endTime")
  List<MovementData> findByTimestampBetween(
      @Param("startTime") Instant startTime,
      @Param("endTime") Instant endTime);

  /**
   * Find movements by destination location
   */
  List<MovementData> findByDestination(String destination);

  /**
   * Find movements containing specific servicing node
   */
  @Query("SELECT m FROM MovementData m WHERE :servicingNode MEMBER OF m.servicingNodes")
  List<MovementData> findByServicingNodesContaining(@Param("servicingNode") String servicingNode);

  /**
   * Count total historical records processed during cut-over
   */
  @Query("SELECT COUNT(m) FROM MovementData m WHERE m.isHistorical = true AND m.processedDuringCutover = true")
  long countHistoricalCutoverRecords();

  /**
   * Find movements by movement type (if tracked)
   */
  @Query("SELECT m FROM MovementData m WHERE m.movementType = :movementType")
  List<MovementData> findByMovementType(@Param("movementType") String movementType);

  /**
   * Find recent movements (last N hours)
   */
  @Query("SELECT m FROM MovementData m WHERE m.timestamp >= :sinceTime ORDER BY m.timestamp DESC")
  List<MovementData> findRecentMovements(@Param("sinceTime") Instant sinceTime);

  /**
   * Check if movement already exists (for duplicate prevention)
   */
  @Query("SELECT m FROM MovementData m WHERE m.sourceLocation = :sourceLocation " +
      "AND m.destinationLocation = :destinationLocation " +
      "AND m.timestamp = :timestamp")
  Optional<MovementData> findExistingMovement(
      @Param("sourceLocation") String sourceLocation,
      @Param("destinationLocation") String destinationLocation,
      @Param("timestamp") Instant timestamp);

  /**
   * Delete old historical records (for cleanup)
   */
  @Query("DELETE FROM MovementData m WHERE m.isHistorical = true AND m.timestamp < :cutoffTime")
  void deleteOldHistoricalRecords(@Param("cutoffTime") Instant cutoffTime);

  /**
   * Get movement statistics for monitoring
   */
  @Query("SELECT " +
      "COUNT(m) as totalCount, " +
      "SUM(CASE WHEN m.isHistorical = true THEN 1 ELSE 0 END) as historicalCount, " +
      "SUM(CASE WHEN m.processedDuringCutover = true THEN 1 ELSE 0 END) as cutoverCount " +
      "FROM MovementData m")
  MovementStatistics getMovementStatistics();

  /**
   * Custom interface for movement statistics
   */
  interface MovementStatistics {
    long getTotalCount();

    long getHistoricalCount();

    long getCutoverCount();
  }
}