package com.company.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service to manage Kafka listener lifecycle
 * Handles pausing and resuming Kafka message processing during cut-over
 * operations
 */
@Slf4j
@Service
public class KafkaListenerService {

  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  private final AtomicBoolean listenersActive = new AtomicBoolean(true);

  /**
   * Pause all Kafka listeners
   * Used during cut-over operations to prevent concurrent message processing
   */
  public void pauseAllListeners() {
    log.info("Pausing all Kafka listeners for cut-over operations");

    try {
      Set<String> listenerIds = kafkaListenerEndpointRegistry.getListenerContainerIds();

      for (String listenerId : listenerIds) {
        MessageListenerContainer container = kafkaListenerEndpointRegistry
            .getListenerContainer(listenerId);

        if (container != null && container.isRunning()) {
          log.debug("Pausing listener: {}", listenerId);
          container.pause();
        } else {
          log.warn("Listener container not found or not running: {}", listenerId);
        }
      }

      listenersActive.set(false);
      log.info("Successfully paused {} Kafka listeners", listenerIds.size());

    } catch (Exception e) {
      log.error("Failed to pause Kafka listeners", e);
      throw new RuntimeException("Failed to pause Kafka listeners during cut-over", e);
    }
  }

  /**
   * Resume all Kafka listeners
   * Used after cut-over operations to restore normal message processing
   */
  public void resumeAllListeners() {
    log.info("Resuming all Kafka listeners after cut-over operations");

    try {
      Set<String> listenerIds = kafkaListenerEndpointRegistry.getListenerContainerIds();

      for (String listenerId : listenerIds) {
        MessageListenerContainer container = kafkaListenerEndpointRegistry
            .getListenerContainer(listenerId);

        if (container != null) {
          log.debug("Resuming listener: {}", listenerId);
          container.resume();
        } else {
          log.warn("Listener container not found: {}", listenerId);
        }
      }

      listenersActive.set(true);
      log.info("Successfully resumed {} Kafka listeners", listenerIds.size());

    } catch (Exception e) {
      log.error("Failed to resume Kafka listeners", e);
      throw new RuntimeException("Failed to resume Kafka listeners after cut-over", e);
    }
  }

  /**
   * Pause specific Kafka listener by ID
   */
  public void pauseListener(String listenerId) {
    log.info("Pausing Kafka listener: {}", listenerId);

    try {
      MessageListenerContainer container = kafkaListenerEndpointRegistry
          .getListenerContainer(listenerId);

      if (container != null && container.isRunning()) {
        container.pause();
        log.info("Successfully paused listener: {}", listenerId);
      } else {
        log.warn("Listener container not found or not running: {}", listenerId);
      }

    } catch (Exception e) {
      log.error("Failed to pause listener: {}", listenerId, e);
      throw new RuntimeException("Failed to pause Kafka listener: " + listenerId, e);
    }
  }

  /**
   * Resume specific Kafka listener by ID
   */
  public void resumeListener(String listenerId) {
    log.info("Resuming Kafka listener: {}", listenerId);

    try {
      MessageListenerContainer container = kafkaListenerEndpointRegistry
          .getListenerContainer(listenerId);

      if (container != null) {
        container.resume();
        log.info("Successfully resumed listener: {}", listenerId);
      } else {
        log.warn("Listener container not found: {}", listenerId);
      }

    } catch (Exception e) {
      log.error("Failed to resume listener: {}", listenerId, e);
      throw new RuntimeException("Failed to resume Kafka listener: " + listenerId, e);
    }
  }

  /**
   * Check if all listeners are currently active
   */
  public boolean areListenersActive() {
    return listenersActive.get();
  }

  /**
   * Get status of all Kafka listeners
   */
  public KafkaListenerStatus getListenerStatus() {
    Set<String> listenerIds = kafkaListenerEndpointRegistry.getListenerContainerIds();

    int totalListeners = listenerIds.size();
    int runningListeners = 0;
    int pausedListeners = 0;

    for (String listenerId : listenerIds) {
      MessageListenerContainer container = kafkaListenerEndpointRegistry
          .getListenerContainer(listenerId);

      if (container != null) {
        if (container.isRunning() && !container.isPauseRequested()) {
          runningListeners++;
        } else {
          pausedListeners++;
        }
      }
    }

    return KafkaListenerStatus.builder()
        .totalListeners(totalListeners)
        .runningListeners(runningListeners)
        .pausedListeners(pausedListeners)
        .allActive(runningListeners == totalListeners)
        .build();
  }

  /**
   * Get detailed information about specific listener
   */
  public ListenerInfo getListenerInfo(String listenerId) {
    MessageListenerContainer container = kafkaListenerEndpointRegistry
        .getListenerContainer(listenerId);

    if (container == null) {
      return ListenerInfo.builder()
          .listenerId(listenerId)
          .exists(false)
          .build();
    }

    return ListenerInfo.builder()
        .listenerId(listenerId)
        .exists(true)
        .running(container.isRunning())
        .paused(container.isPauseRequested())
        .autoStartup(container.isAutoStartup())
        .phase(container.getPhase())
        .build();
  }

  /**
   * Emergency stop all listeners (for critical situations)
   */
  public void emergencyStopAllListeners() {
    log.error("EMERGENCY: Stopping all Kafka listeners immediately");

    try {
      Set<String> listenerIds = kafkaListenerEndpointRegistry.getListenerContainerIds();

      for (String listenerId : listenerIds) {
        MessageListenerContainer container = kafkaListenerEndpointRegistry
            .getListenerContainer(listenerId);

        if (container != null && container.isRunning()) {
          log.warn("Emergency stopping listener: {}", listenerId);
          container.stop();
        }
      }

      listenersActive.set(false);
      log.error("Emergency stop completed for {} listeners", listenerIds.size());

    } catch (Exception e) {
      log.error("Failed during emergency stop of Kafka listeners", e);
      throw new RuntimeException("Emergency stop of Kafka listeners failed", e);
    }
  }

  /**
   * Data class for listener status information
   */
  @lombok.Data
  @lombok.Builder
  @lombok.NoArgsConstructor
  @lombok.AllArgsConstructor
  public static class KafkaListenerStatus {
    private int totalListeners;
    private int runningListeners;
    private int pausedListeners;
    private boolean allActive;
  }

  /**
   * Data class for individual listener information
   */
  @lombok.Data
  @lombok.Builder
  @lombok.NoArgsConstructor
  @lombok.AllArgsConstructor
  public static class ListenerInfo {
    private String listenerId;
    private boolean exists;
    private boolean running;
    private boolean paused;
    private boolean autoStartup;
    private int phase;
  }
}