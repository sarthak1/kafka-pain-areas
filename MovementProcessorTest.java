package com.company.kafka.movement;

import com.company.kafka.service.LocationService;
import com.company.kafka.service.MovementValidationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test class for MovementProcessor
 * Tests both normal and reverse movement scenarios
 */
@ExtendWith(MockitoExtension.class)
class MovementProcessorTest {

  @Mock
  private LocationService locationService;

  @Mock
  private MovementValidationService validationService;

  @InjectMocks
  private MovementProcessor movementProcessor;

  private MovementProcessor.KafkaMovementMessage normalMovementMessage;
  private MovementProcessor.KafkaMovementMessage reverseMovementMessage;

  @BeforeEach
  void setUp() {
    // Normal movement: DC to Store
    normalMovementMessage = new MovementProcessor.KafkaMovementMessage();
    normalMovementMessage.setDestination("2352");
    normalMovementMessage.setServicingNodes(Arrays.asList("960", "1001", "1002"));
    normalMovementMessage.setSourceLocation("960");
    normalMovementMessage.setDestinationLocation("2352");
    normalMovementMessage.setStatus("PLANNED");
    normalMovementMessage.setTimestamp(Instant.now());

    // Reverse movement: Store to DC
    reverseMovementMessage = new MovementProcessor.KafkaMovementMessage();
    reverseMovementMessage.setDestination("960");
    reverseMovementMessage.setServicingNodes(Arrays.asList("960", "1001", "1002"));
    reverseMovementMessage.setSourceLocation("2352");
    reverseMovementMessage.setDestinationLocation("960");
    reverseMovementMessage.setStatus("PLANNED");
    reverseMovementMessage.setTimestamp(Instant.now());

    // Mock validation service to return valid results
    MovementProcessor.ValidationResult validResult = MovementProcessor.ValidationResult.builder()
        .valid(true)
        .validationCode("VALID")
        .build();
    when(validationService.validate(any())).thenReturn(validResult);
  }

  @Test
  void testNormalMovement_DCToStore() {
    // Given
    when(locationService.getLocationType("960")).thenReturn(LocationService.LocationType.DC);
    when(locationService.getLocationType("2352")).thenReturn(LocationService.LocationType.STORE);

    // When
    MovementProcessor.ProcessedMovement result = movementProcessor.processMovement(normalMovementMessage);

    // Then
    assertNotNull(result);
    assertEquals(MovementProcessor.MovementType.NORMAL, result.getMovementType());
    assertEquals("960", result.getActualOrigin());
    assertEquals("2352", result.getActualDestination());
    assertEquals("DC_TO_STORE", result.getFlowDirection());
    assertTrue(result.getValidation().isValid());

    verify(locationService).getLocationType("960");
    verify(locationService).getLocationType("2352");
    verify(validationService).validate(result);
  }

  @Test
  void testReverseMovement_StoreToDC() {
    // Given
    when(locationService.getLocationType("2352")).thenReturn(LocationService.LocationType.STORE);
    when(locationService.getLocationType("960")).thenReturn(LocationService.LocationType.DC);

    // When
    MovementProcessor.ProcessedMovement result = movementProcessor.processMovement(reverseMovementMessage);

    // Then
    assertNotNull(result);
    assertEquals(MovementProcessor.MovementType.REVERSE, result.getMovementType());
    assertEquals("960", result.getActualOrigin()); // Swapped - now DC is origin
    assertEquals("2352", result.getActualDestination()); // Swapped - now Store is destination
    assertEquals("STORE_TO_DC", result.getFlowDirection());
    assertTrue(result.getValidation().isValid());

    verify(locationService).getLocationType("2352");
    verify(locationService).getLocationType("960");
    verify(validationService).validate(result);
  }

  @Test
  void testReverseMovement_DetectedByServicingNodes() {
    // Given - both locations return UNKNOWN, so fallback to servicing nodes check
    when(locationService.getLocationType(any())).thenReturn(LocationService.LocationType.UNKNOWN);

    MovementProcessor.KafkaMovementMessage messageWithDestinationInServicingNodes = new MovementProcessor.KafkaMovementMessage();
    messageWithDestinationInServicingNodes.setDestination("960");
    messageWithDestinationInServicingNodes.setServicingNodes(Arrays.asList("960", "1001", "1002"));
    messageWithDestinationInServicingNodes.setSourceLocation("2352");
    messageWithDestinationInServicingNodes.setDestinationLocation("960");
    messageWithDestinationInServicingNodes.setStatus("PLANNED");
    messageWithDestinationInServicingNodes.setTimestamp(Instant.now());

    // When
    MovementProcessor.ProcessedMovement result = movementProcessor
        .processMovement(messageWithDestinationInServicingNodes);

    // Then
    assertNotNull(result);
    assertEquals(MovementProcessor.MovementType.REVERSE, result.getMovementType());
    assertEquals("960", result.getActualOrigin()); // destinationLocation becomes actualOrigin
    assertEquals("2352", result.getActualDestination()); // sourceLocation becomes actualDestination
    assertEquals("STORE_TO_DC", result.getFlowDirection());
  }

  @Test
  void testMultipleNormalMovements() {
    // Given
    when(locationService.getLocationType("960")).thenReturn(LocationService.LocationType.DC);
    when(locationService.getLocationType("2352")).thenReturn(LocationService.LocationType.STORE);

    List<MovementProcessor.KafkaMovementMessage> messages = Arrays.asList(
        normalMovementMessage,
        createMessage("961", "2353", "961", "2353"),
        createMessage("962", "2354", "962", "2354"));

    // When
    List<MovementProcessor.ProcessedMovement> results = messages.stream()
        .map(movementProcessor::processMovement)
        .toList();

    // Then
    assertEquals(3, results.size());
    results.forEach(result -> {
      assertEquals(MovementProcessor.MovementType.NORMAL, result.getMovementType());
      assertEquals("DC_TO_STORE", result.getFlowDirection());
      assertTrue(result.getValidation().isValid());
    });

    verify(locationService, times(6)).getLocationType(any());
    verify(validationService, times(3)).validate(any());
  }

  @Test
  void testMixedMovementTypes() {
    // Given
    when(locationService.getLocationType("960")).thenReturn(LocationService.LocationType.DC);
    when(locationService.getLocationType("2352")).thenReturn(LocationService.LocationType.STORE);

    List<MovementProcessor.KafkaMovementMessage> messages = Arrays.asList(
        normalMovementMessage, // DC to Store (normal)
        reverseMovementMessage // Store to DC (reverse)
    );

    // When
    List<MovementProcessor.ProcessedMovement> results = messages.stream()
        .map(movementProcessor::processMovement)
        .toList();

    // Then
    assertEquals(2, results.size());

    // First result - normal movement
    MovementProcessor.ProcessedMovement normalResult = results.get(0);
    assertEquals(MovementProcessor.MovementType.NORMAL, normalResult.getMovementType());
    assertEquals("960", normalResult.getActualOrigin());
    assertEquals("2352", normalResult.getActualDestination());

    // Second result - reverse movement
    MovementProcessor.ProcessedMovement reverseResult = results.get(1);
    assertEquals(MovementProcessor.MovementType.REVERSE, reverseResult.getMovementType());
    assertEquals("960", reverseResult.getActualOrigin()); // Swapped
    assertEquals("2352", reverseResult.getActualDestination()); // Swapped
  }

  @Test
  void testValidationFailure() {
    // Given
    when(locationService.getLocationType("960")).thenReturn(LocationService.LocationType.DC);
    when(locationService.getLocationType("2352")).thenReturn(LocationService.LocationType.STORE);

    MovementProcessor.ValidationResult invalidResult = MovementProcessor.ValidationResult.builder()
        .valid(false)
        .errorMessage("Invalid movement data")
        .validationCode("INVALID_DATA")
        .build();
    when(validationService.validate(any())).thenReturn(invalidResult);

    // When
    MovementProcessor.ProcessedMovement result = movementProcessor.processMovement(normalMovementMessage);

    // Then
    assertNotNull(result);
    assertFalse(result.getValidation().isValid());
    assertEquals("Invalid movement data", result.getValidation().getErrorMessage());
    assertEquals("INVALID_DATA", result.getValidation().getValidationCode());
  }

  private MovementProcessor.KafkaMovementMessage createMessage(
      String sourceLocation, String destinationLocation,
      String destination, String actualDestination) {
    MovementProcessor.KafkaMovementMessage message = new MovementProcessor.KafkaMovementMessage();
    message.setDestination(destination);
    message.setServicingNodes(Arrays.asList(sourceLocation, "1001", "1002"));
    message.setSourceLocation(sourceLocation);
    message.setDestinationLocation(actualDestination);
    message.setStatus("PLANNED");
    message.setTimestamp(Instant.now());
    return message;
  }
}