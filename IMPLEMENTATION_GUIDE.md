# Implementation Guide for Kafka Pain Areas

## Quick Summary

This repository contains solutions for two critical Kafka system pain areas:

### Pain Area 1: Reverse Movement Detection ✅
**Problem**: When items move from Store → DC (reverse), the payload format remains the same, causing incorrect origin/destination mapping.

**Solution**: `MovementProcessor.java`
- Detects movement type using business rules (Store→DC = reverse)
- Fallback detection using servicing nodes
- Swaps semantic meaning of origin/destination for reverse movements
- Maintains data integrity with validation

### Pain Area 2: Cut-over Historical Data Gap ✅
**Problem**: When system goes live, cannot access historical Kafka messages published before startup.

**Solution**: `CutoverDataService.java`
- Dual-source strategy: REST API for historical data + Kafka for real-time
- Configurable gap period processing
- Parallel data fetching for performance
- Automatic system pause/resume during cut-over

## Quick Start

### 1. Configuration Setup
```properties
# Enable cut-over mode
system.cutover.enabled=true
system.cutover.gap-days=30

# Configure SA team REST API
system.cutover.rest-api.base-url=https://sa-team-api.company.com/api/v1

# Movement detection strategy
movement.detection.strategy=BUSINESS_RULE
```

### 2. Movement Processing Usage
```java
@Autowired
private MovementProcessor movementProcessor;

@KafkaListener(topics = "sa-team-topic")
public void handleMovement(KafkaMovementMessage message) {
    ProcessedMovement processed = movementProcessor.processMovement(message);
    
    if (processed.getValidation().isValid()) {
        // Use processed.getActualOrigin() and processed.getActualDestination()
        // These are correctly mapped regardless of movement type
        processValidMovement(processed);
    } else {
        handleInvalidMovement(processed);
    }
}
```

### 3. Cut-over Execution
The cut-over process runs automatically on application startup when enabled:
- Fetches historical data from REST API
- Processes and validates data
- Stores in database with cut-over flags
- Resumes normal Kafka processing

## Key Features

### Reverse Movement Detection
- ✅ Business rule based detection (Store → DC = reverse)
- ✅ Servicing nodes fallback detection
- ✅ Automatic origin/destination swapping
- ✅ Flow direction tracking
- ✅ Comprehensive validation

### Historical Data Processing
- ✅ REST API integration for historical data
- ✅ Configurable gap period (default 30 days)
- ✅ Parallel processing for performance
- ✅ Batch processing to handle large datasets
- ✅ Error handling and retry logic
- ✅ Data validation and integrity checks

### Monitoring & Observability
- ✅ Comprehensive logging
- ✅ Processing metrics
- ✅ Error tracking
- ✅ Performance monitoring
- ✅ Cut-over progress tracking

## Testing

Run the comprehensive test suite:
```bash
mvn test
```

Key test scenarios covered:
- Normal DC→Store movements
- Reverse Store→DC movements
- Mixed movement types
- Validation failures
- Edge cases and error scenarios

## Architecture Benefits

1. **Data Integrity**: Ensures correct origin/destination mapping regardless of movement direction
2. **Zero Data Loss**: Historical gap coverage through REST API fallback
3. **Performance**: Parallel processing for large data volumes
4. **Reliability**: Comprehensive error handling and validation
5. **Observability**: Full monitoring and alerting capabilities
6. **Flexibility**: Configurable strategies and parameters

## Production Deployment

### Phase 1: Movement Detection
1. Deploy movement processor service
2. Configure location service integration
3. Enable monitoring and alerting
4. Test with sample data

### Phase 2: Cut-over Implementation  
1. Configure REST API endpoints
2. Set cut-over parameters
3. Plan maintenance window
4. Execute cut-over with monitoring

### Phase 3: Validation
1. Validate historical data processing
2. Verify real-time processing
3. Monitor system performance
4. Adjust configurations as needed

## Key Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `system.cutover.enabled` | Enable cut-over mode | false |
| `system.cutover.gap-days` | Historical data gap period | 30 |
| `system.cutover.rest-api.batch-size` | REST API batch size | 1000 |
| `movement.detection.strategy` | Detection strategy | BUSINESS_RULE |
| `movement.reverse.validation.enabled` | Enable reverse validation | true |

This solution provides a robust, scalable approach to handling both Kafka pain areas while maintaining system reliability and data integrity.