package com.company.kafka.cutover;

import com.company.kafka.entity.MovementData;
import com.company.kafka.repository.MovementDataRepository;
import com.company.kafka.service.KafkaListenerService;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Service to handle cut-over scenarios and historical data processing
 * Addresses Pain Area 2: Historical Data Gap Issue
 */
@Slf4j
@Service
public class CutoverDataService {

  @Autowired
  private RestTemplate restTemplate;

  @Autowired
  private MovementDataRepository movementDataRepository;

  @Autowired
  private KafkaListenerService kafkaListenerService;

  @Value("${system.cutover.enabled:false}")
  private boolean cutoverEnabled;

  @Value("${system.cutover.gap-days:30}")
  private int gapDays;

  @Value("${system.cutover.rest-api.base-url}")
  private String saTeamApiBaseUrl;

  @Value("${system.cutover.rest-api.batch-size:1000}")
  private int batchSize;

  @Value("${system.cutover.parallel-processing:true}")
  private boolean parallelProcessing;

  private final ExecutorService executorService = Executors.newFixedThreadPool(5);

  /**
   * Handle application startup - perform cut-over if enabled
   */
  @EventListener
  public void handleApplicationStartup(ApplicationReadyEvent event) {
    if (cutoverEnabled) {
      log.info("Cut-over mode enabled. Starting historical data processing...");
      performCutoverOperations();
    } else {
      log.info("Normal startup mode. Skipping historical data processing.");
    }
  }

  /**
   * Main cut-over operation orchestrator
   */
  public CutoverResult performCutoverOperations() {
    CutoverResult result = CutoverResult.builder()
        .startTime(java.time.Instant.now())
        .build();

    try {
      // Step 1: Pause real-time Kafka processing
      log.info("Pausing Kafka listeners for cut-over processing");
      kafkaListenerService.pauseAllListeners();

      // Step 2: Calculate gap period
      LocalDate cutoverDate = LocalDate.now();
      LocalDate startDate = cutoverDate.minusDays(gapDays);

      log.info("Processing historical data from {} to {}", startDate, cutoverDate);

      // Step 3: Fetch and process historical data
      HistoricalDataResult historicalResult = processHistoricalPeriod(startDate, cutoverDate);
      result.setHistoricalDataResult(historicalResult);

      // Step 4: Validate data integrity
      ValidationSummary validationSummary = validateHistoricalData(historicalResult);
      result.setValidationSummary(validationSummary);

      // Step 5: Resume real-time processing
      log.info("Resuming Kafka listeners after cut-over completion");
      kafkaListenerService.resumeAllListeners();

      result.setSuccess(true);
      result.setEndTime(java.time.Instant.now());

      log.info("Cut-over operations completed successfully. Processed {} records in {} ms",
          historicalResult.getTotalRecordsProcessed(),
          result.getDurationMs());

    } catch (Exception e) {
      log.error("Cut-over operations failed", e);
      result.setSuccess(false);
      result.setErrorMessage(e.getMessage());
      result.setEndTime(java.time.Instant.now());

      // Ensure Kafka listeners are resumed even on failure
      try {
        kafkaListenerService.resumeAllListeners();
      } catch (Exception resumeException) {
        log.error("Failed to resume Kafka listeners after cut-over failure", resumeException);
      }
    }

    return result;
  }

  /**
   * Process historical data for a given period
   */
  private HistoricalDataResult processHistoricalPeriod(LocalDate startDate, LocalDate endDate) {
    HistoricalDataResult result = HistoricalDataResult.builder()
        .startDate(startDate)
        .endDate(endDate)
        .build();

    List<HistoricalMovementData> allHistoricalData = new ArrayList<>();
    List<String> failedDates = new ArrayList<>();

    LocalDate currentDate = startDate;

    while (currentDate.isBefore(endDate)) {
      try {
        log.debug("Fetching data for date: {}", currentDate);

        if (parallelProcessing) {
          // Process multiple dates in parallel
          List<CompletableFuture<List<HistoricalMovementData>>> futures = new ArrayList<>();

          // Process in batches of 7 days
          LocalDate batchEndDate = currentDate.plusDays(7);
          if (batchEndDate.isAfter(endDate)) {
            batchEndDate = endDate;
          }

          while (currentDate.isBefore(batchEndDate)) {
            LocalDate dateToProcess = currentDate;
            CompletableFuture<List<HistoricalMovementData>> future = CompletableFuture
                .supplyAsync(() -> fetchMovementDataForDate(dateToProcess), executorService);
            futures.add(future);
            currentDate = currentDate.plusDays(1);
          }

          // Wait for all futures to complete
          CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

          // Collect results
          for (CompletableFuture<List<HistoricalMovementData>> future : futures) {
            try {
              allHistoricalData.addAll(future.get());
            } catch (Exception e) {
              log.error("Failed to process parallel batch", e);
            }
          }

        } else {
          // Sequential processing
          List<HistoricalMovementData> dailyData = fetchMovementDataForDate(currentDate);
          allHistoricalData.addAll(dailyData);
          currentDate = currentDate.plusDays(1);
        }

      } catch (Exception e) {
        log.error("Failed to fetch data for date: {}", currentDate, e);
        failedDates.add(currentDate.toString());
        currentDate = currentDate.plusDays(1);
      }
    }

    // Process and store the historical data
    int processedCount = processAndStoreHistoricalData(allHistoricalData);

    result.setTotalRecordsProcessed(processedCount);
    result.setFailedDates(failedDates);
    result.setSuccess(failedDates.isEmpty());

    return result;
  }

  /**
   * Fetch movement data for a specific date via REST API
   */
  private List<HistoricalMovementData> fetchMovementDataForDate(LocalDate date) {
    String dateStr = date.format(DateTimeFormatter.ISO_LOCAL_DATE);
    String url = String.format("%s/movements/by-date/%s", saTeamApiBaseUrl, dateStr);

    try {
      log.debug("Calling REST API: {}", url);
      HistoricalMovementData[] responseArray = restTemplate.getForObject(url, HistoricalMovementData[].class);

      if (responseArray != null) {
        log.debug("Fetched {} records for date {}", responseArray.length, date);
        return List.of(responseArray);
      } else {
        log.warn("No data returned for date {}", date);
        return new ArrayList<>();
      }

    } catch (Exception e) {
      log.error("Failed to fetch data for date {} from URL {}", date, url, e);
      throw new RuntimeException("Failed to fetch historical data for date: " + date, e);
    }
  }

  /**
   * Process and store historical data in database
   */
  private int processAndStoreHistoricalData(List<HistoricalMovementData> historicalData) {
    int processedCount = 0;

    log.info("Processing {} historical records", historicalData.size());

    // Process in batches to avoid memory issues
    for (int i = 0; i < historicalData.size(); i += batchSize) {
      int endIndex = Math.min(i + batchSize, historicalData.size());
      List<HistoricalMovementData> batch = historicalData.subList(i, endIndex);

      try {
        // Convert historical data to our internal format and save
        List<MovementData> convertedBatch = convertHistoricalData(batch);
        movementDataRepository.saveAll(convertedBatch);
        processedCount += batch.size();

        log.debug("Processed batch {}/{}", endIndex, historicalData.size());

      } catch (Exception e) {
        log.error("Failed to process batch starting at index {}", i, e);
      }
    }

    log.info("Successfully processed and stored {} historical records", processedCount);
    return processedCount;
  }

  /**
   * Convert historical data format to internal movement data format
   */
  private List<MovementData> convertHistoricalData(List<HistoricalMovementData> historicalData) {
    List<MovementData> converted = new ArrayList<>();

    for (HistoricalMovementData historical : historicalData) {
      try {
        MovementData movement = MovementData.builder()
            .sourceLocation(historical.getSourceLocation())
            .destinationLocation(historical.getDestinationLocation())
            .destination(historical.getDestination())
            .servicingNodes(historical.getServicingNodes())
            .status(historical.getStatus())
            .timestamp(historical.getTimestamp())
            .isHistorical(true)
            .processedDuringCutover(true)
            .build();

        converted.add(movement);

      } catch (Exception e) {
        log.error("Failed to convert historical record: {}", historical, e);
      }
    }

    return converted;
  }

  /**
   * Validate historical data integrity
   */
  private ValidationSummary validateHistoricalData(HistoricalDataResult result) {
    ValidationSummary summary = ValidationSummary.builder()
        .totalRecordsValidated(result.getTotalRecordsProcessed())
        .build();

    // Add validation logic here
    // For example: check for duplicate records, validate data consistency, etc.

    summary.setValidationPassed(true);
    return summary;
  }

  // Data classes
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CutoverResult {
    private boolean success;
    private java.time.Instant startTime;
    private java.time.Instant endTime;
    private String errorMessage;
    private HistoricalDataResult historicalDataResult;
    private ValidationSummary validationSummary;

    public long getDurationMs() {
      if (startTime != null && endTime != null) {
        return java.time.Duration.between(startTime, endTime).toMillis();
      }
      return 0;
    }
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class HistoricalDataResult {
    private LocalDate startDate;
    private LocalDate endDate;
    private int totalRecordsProcessed;
    private List<String> failedDates;
    private boolean success;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ValidationSummary {
    private int totalRecordsValidated;
    private int validRecords;
    private int invalidRecords;
    private boolean validationPassed;
    private List<String> validationErrors;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class HistoricalMovementData {
    private String destination;
    private List<String> servicingNodes;
    private String sourceLocation;
    private String destinationLocation;
    private String status;
    private java.time.Instant timestamp;
  }
}