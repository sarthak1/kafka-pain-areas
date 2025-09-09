# Kafka Pain Areas - Complete Solution

## Overview

This project provides a comprehensive solution for two critical Kafka system pain areas:

1. **Reverse Movement Handling** - Correctly processing movements when items flow from Store → DC (reverse of normal DC → Store)
2. **Cut-over Historical Data Gap** - Handling historical data that was published before the system went live

## 📁 Project Structure

```
/Users/sarthak/CascadeProjects/kafka_pain_areas/
├── README.md                           # This file
├── IMPLEMENTATION_GUIDE.md             # Quick start guide
├── kafka_pain_areas_analysis.md        # Detailed analysis
├── application.properties              # Configuration
├── KafkaPainAreasConfig.java           # Spring configuration
│
├── Core Components/
│   ├── MovementProcessor.java          # Pain Area 1: Reverse movement detection
│   ├── CutoverDataService.java         # Pain Area 2: Historical data handling
│   ├── MovementData.java               # JPA entity for movement data
│   ├── MovementDataRepository.java     # Data access layer
│   │
├── Services/
│   ├── LocationService.java            # Location type detection
│   ├── MovementValidationService.java  # Business rule validation
│   ├── KafkaListenerService.java       # Kafka lifecycle management
│   │
└── Tests/
    └── MovementProcessorTest.java      # Comprehensive test suite
```

## 🚀 Quick Start

### 1. Configuration
Update [application.properties](file:///Users/sarthak/CascadeProjects/kafka_pain_areas/application.properties):

```properties
# Enable cut-over mode for first-time deployment
system.cutover.enabled=true
system.cutover.gap-days=30

# Configure SA team REST API
system.cutover.rest-api.base-url=https://sa-team-api.company.com/api/v1

# Movement detection strategy
movement.detection.strategy=BUSINESS_RULE
```

### 2. Core Usage

#### Reverse Movement Detection
```java
@Autowired
private MovementProcessor movementProcessor;

@KafkaListener(topics = "sa-team-topic")
public void handleMovement(KafkaMovementMessage message) {
    ProcessedMovement processed = movementProcessor.processMovement(message);
    
    // Correctly mapped origin/destination regardless of movement direction
    String actualOrigin = processed.getActualOrigin();
    String actualDestination = processed.getActualDestination();
    MovementType type = processed.getMovementType(); // NORMAL or REVERSE
}
```

#### Cut-over Processing
The cut-over process runs automatically on application startup when enabled. Monitor logs for progress:

```
Cut-over mode enabled. Starting historical data processing...
Processing historical data from 2024-08-15 to 2024-09-15
Fetched 1500 records for date 2024-08-15
Successfully processed and stored 45000 historical records
Cut-over operations completed successfully
```

## 🔧 Key Components

### 1. MovementProcessor
- **Purpose**: Handles Pain Area 1 (Reverse Movement Detection)
- **Strategy**: Business rule based detection + servicing nodes fallback
- **Output**: Correctly mapped movement with actual origin/destination

### 2. CutoverDataService  
- **Purpose**: Handles Pain Area 2 (Historical Data Gap)
- **Strategy**: Dual-source approach (REST API + Kafka)
- **Features**: Parallel processing, error handling, automatic orchestration

### 3. LocationService
- **Purpose**: Determines location types (Store, DC, Unknown)
- **Methods**: Pattern matching, known locations cache, prefix detection

### 4. MovementValidationService
- **Purpose**: Validates processed movements against business rules
- **Checks**: Required fields, location consistency, timestamp validity

## 📊 Data Flow

### Normal Movement (DC → Store)
```
Original Message: {source: "960", destination: "2352", servicingNodes: ["960", "1001"]}
↓
Detection: LocationService identifies 960=DC, 2352=Store  
↓
Processing: MovementType.NORMAL, actualOrigin="960", actualDestination="2352"
↓
Validation: Passes business rules
↓
Storage: Saved with flowDirection="DC_TO_STORE"
```

### Reverse Movement (Store → DC)
```
Original Message: {source: "2352", destination: "960", servicingNodes: ["960", "1001"]}
↓
Detection: LocationService identifies 2352=Store, 960=DC
↓
Processing: MovementType.REVERSE, actualOrigin="960", actualDestination="2352" (SWAPPED!)
↓
Validation: Passes reverse movement rules
↓
Storage: Saved with flowDirection="STORE_TO_DC"
```

### Cut-over Process
```
Application Startup
↓
Pause Kafka Listeners
↓
Fetch Historical Data (REST API): Aug 15 - Sep 15
↓
Process & Validate: 45,000 records
↓
Store with Historical Flags
↓
Resume Kafka Listeners
↓
Normal Real-time Processing
```

## 🧪 Testing

Run the test suite:
```bash
mvn test
```

### Test Coverage
- ✅ Normal DC→Store movements
- ✅ Reverse Store→DC movements  
- ✅ Mixed movement batches
- ✅ Validation failures
- ✅ Edge cases and error scenarios
- ✅ Location type detection
- ✅ Cut-over process simulation

## 📈 Monitoring

### Key Metrics
- Movement processing rate
- Reverse movement detection percentage
- Validation success rate
- Cut-over progress and completion time
- Historical data processing volume

### Alerts
- High reverse movement rate (potential anomaly)
- Validation failure spikes
- Cut-over process failures
- Kafka processing lag

## 🔧 Configuration Reference

| Property | Description | Default |
|----------|-------------|---------|
| `system.cutover.enabled` | Enable cut-over mode | `false` |
| `system.cutover.gap-days` | Historical data gap period | `30` |
| `system.cutover.rest-api.base-url` | SA team API endpoint | Required |
| `system.cutover.parallel-processing` | Enable parallel data fetching | `true` |
| `movement.detection.strategy` | Detection strategy | `BUSINESS_RULE` |
| `movement.validation.strict-mode` | Enable strict validation | `false` |

## 🚀 Production Deployment

### Phase 1: Movement Detection
1. Deploy core components
2. Configure location mappings
3. Enable monitoring
4. Test with sample data

### Phase 2: Cut-over Implementation
1. Configure REST API access
2. Set cut-over parameters
3. Plan maintenance window
4. Execute with monitoring

### Phase 3: Validation & Optimization
1. Validate data accuracy
2. Monitor performance
3. Tune configuration
4. Document lessons learned

## 📋 Dependencies

### Required
- Spring Boot 2.5+
- Spring Kafka
- Spring Data JPA
- PostgreSQL/MySQL
- Lombok

### Optional
- Spring Boot Actuator (monitoring)
- Micrometer (metrics)
- Logback (logging)

## 🔍 Troubleshooting

### Common Issues

1. **Cut-over fails to fetch historical data**
   - Check REST API connectivity and credentials
   - Verify date format and endpoint URLs
   - Review API rate limits

2. **Reverse movements not detected correctly**
   - Verify location service mappings
   - Check servicing nodes data quality
   - Review detection strategy configuration

3. **High validation failure rate**
   - Check timestamp ranges
   - Verify location format patterns
   - Review business rule configuration

### Debug Mode
Enable debug logging:
```properties
logging.level.com.company.kafka=DEBUG
```

## 📝 Next Steps

1. **Enhanced Location Detection**: Integrate with master data service
2. **Real-time Monitoring**: Add Grafana dashboards
3. **Automated Alerts**: Configure PagerDuty/Slack notifications
4. **Performance Optimization**: Implement caching strategies
5. **Data Quality**: Add data reconciliation reports

## 🤝 Contributing

1. Follow existing code patterns and naming conventions
2. Add comprehensive unit tests for new features
3. Update documentation for configuration changes
4. Test cut-over scenarios in staging environment

This solution provides a robust, production-ready approach to handling both Kafka pain areas while maintaining data integrity and system reliability.