package com.company.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service to manage location type information
 * Determines if a location is a Store, Distribution Center, or Unknown
 */
@Slf4j
@Service
public class LocationService {

  @Value("${location.service.cache.enabled:true}")
  private boolean cacheEnabled;

  @Value("${location.service.default.store.prefix:23}")
  private String defaultStorePrefix;

  @Value("${location.service.default.dc.prefix:9}")
  private String defaultDcPrefix;

  // Cache for location types to avoid repeated lookups
  private final Map<String, LocationType> locationTypeCache = new ConcurrentHashMap<>();

  // Static mapping for known locations (in real implementation, this would come
  // from database/API)
  private final Map<String, LocationType> knownLocations = new HashMap<>();

  @PostConstruct
  public void initializeKnownLocations() {
    log.info("Initializing LocationService with cache enabled: {}", cacheEnabled);

    // Initialize known locations (in real implementation, load from
    // database/external service)
    populateKnownLocations();

    log.info("LocationService initialized with {} known locations", knownLocations.size());
  }

  /**
   * Get location type for a given location ID
   */
  public LocationType getLocationType(String locationId) {
    if (locationId == null || locationId.trim().isEmpty()) {
      log.warn("Invalid location ID provided: {}", locationId);
      return LocationType.UNKNOWN;
    }

    String trimmedLocationId = locationId.trim();

    // Check cache first if enabled
    if (cacheEnabled && locationTypeCache.containsKey(trimmedLocationId)) {
      LocationType cachedType = locationTypeCache.get(trimmedLocationId);
      log.debug("Location type found in cache: {} -> {}", trimmedLocationId, cachedType);
      return cachedType;
    }

    // Determine location type
    LocationType locationType = determineLocationType(trimmedLocationId);

    // Cache the result if caching is enabled
    if (cacheEnabled) {
      locationTypeCache.put(trimmedLocationId, locationType);
    }

    log.debug("Determined location type: {} -> {}", trimmedLocationId, locationType);
    return locationType;
  }

  /**
   * Core logic to determine location type
   */
  private LocationType determineLocationType(String locationId) {
    // Strategy 1: Check known locations first
    if (knownLocations.containsKey(locationId)) {
      return knownLocations.get(locationId);
    }

    // Strategy 2: Pattern-based detection
    LocationType typeByPattern = detectLocationTypeByPattern(locationId);
    if (typeByPattern != LocationType.UNKNOWN) {
      return typeByPattern;
    }

    // Strategy 3: Prefix-based detection (fallback)
    return detectLocationTypeByPrefix(locationId);
  }

  /**
   * Detect location type based on known patterns
   */
  private LocationType detectLocationTypeByPattern(String locationId) {
    try {
      // Pattern 1: 4-digit numbers starting with 2 are typically stores
      if (locationId.matches("^2\\d{3}$")) {
        return LocationType.STORE;
      }

      // Pattern 2: 3-digit numbers starting with 9 are typically DCs
      if (locationId.matches("^9\\d{2}$")) {
        return LocationType.DC;
      }

      // Pattern 3: 4-digit numbers starting with 1 are typically DCs
      if (locationId.matches("^1\\d{3}$")) {
        return LocationType.DC;
      }

      // Add more patterns as needed based on business rules

    } catch (Exception e) {
      log.warn("Error in pattern detection for location: {}", locationId, e);
    }

    return LocationType.UNKNOWN;
  }

  /**
   * Detect location type based on configured prefixes (fallback method)
   */
  private LocationType detectLocationTypeByPrefix(String locationId) {
    try {
      if (locationId.startsWith(defaultStorePrefix)) {
        return LocationType.STORE;
      }

      if (locationId.startsWith(defaultDcPrefix)) {
        return LocationType.DC;
      }

    } catch (Exception e) {
      log.warn("Error in prefix detection for location: {}", locationId, e);
    }

    return LocationType.UNKNOWN;
  }

  /**
   * Populate known locations (in real implementation, this would load from
   * database/API)
   */
  private void populateKnownLocations() {
    // Example known stores
    knownLocations.put("2352", LocationType.STORE);
    knownLocations.put("2353", LocationType.STORE);
    knownLocations.put("2354", LocationType.STORE);
    knownLocations.put("2355", LocationType.STORE);

    // Example known DCs
    knownLocations.put("960", LocationType.DC);
    knownLocations.put("961", LocationType.DC);
    knownLocations.put("962", LocationType.DC);
    knownLocations.put("1001", LocationType.DC);
    knownLocations.put("1002", LocationType.DC);
    knownLocations.put("1003", LocationType.DC);

    log.debug("Populated {} known locations", knownLocations.size());
  }

  /**
   * Add a new location to the known locations
   */
  public void addKnownLocation(String locationId, LocationType locationType) {
    if (locationId != null && locationType != null) {
      knownLocations.put(locationId.trim(), locationType);

      // Update cache if enabled
      if (cacheEnabled) {
        locationTypeCache.put(locationId.trim(), locationType);
      }

      log.info("Added known location: {} -> {}", locationId, locationType);
    }
  }

  /**
   * Bulk add known locations
   */
  public void addKnownLocations(Map<String, LocationType> locations) {
    if (locations != null && !locations.isEmpty()) {
      knownLocations.putAll(locations);

      if (cacheEnabled) {
        locationTypeCache.putAll(locations);
      }

      log.info("Added {} known locations", locations.size());
    }
  }

  /**
   * Check if a location is a store
   */
  public boolean isStore(String locationId) {
    return getLocationType(locationId) == LocationType.STORE;
  }

  /**
   * Check if a location is a distribution center
   */
  public boolean isDistributionCenter(String locationId) {
    return getLocationType(locationId) == LocationType.DC;
  }

  /**
   * Check if location type is unknown
   */
  public boolean isUnknownLocation(String locationId) {
    return getLocationType(locationId) == LocationType.UNKNOWN;
  }

  /**
   * Clear the location type cache
   */
  public void clearCache() {
    if (cacheEnabled) {
      locationTypeCache.clear();
      log.info("Location type cache cleared");
    }
  }

  /**
   * Get cache statistics
   */
  public CacheStatistics getCacheStatistics() {
    return CacheStatistics.builder()
        .enabled(cacheEnabled)
        .size(locationTypeCache.size())
        .knownLocations(knownLocations.size())
        .build();
  }

  /**
   * Validate if a movement between two locations makes sense
   */
  public boolean isValidMovement(String sourceLocation, String destinationLocation) {
    LocationType sourceType = getLocationType(sourceLocation);
    LocationType destType = getLocationType(destinationLocation);

    // Unknown locations are considered valid (benefit of doubt)
    if (sourceType == LocationType.UNKNOWN || destType == LocationType.UNKNOWN) {
      return true;
    }

    // Store to Store movements are typically invalid
    if (sourceType == LocationType.STORE && destType == LocationType.STORE) {
      log.warn("Invalid store-to-store movement detected: {} -> {}", sourceLocation, destinationLocation);
      return false;
    }

    // DC to DC movements might be valid (transfer between DCs)
    // Store to DC and DC to Store movements are valid
    return true;
  }

  /**
   * Enum for location types
   */
  public enum LocationType {
    STORE,
    DC,
    UNKNOWN
  }

  /**
   * Cache statistics data class
   */
  @lombok.Data
  @lombok.Builder
  @lombok.NoArgsConstructor
  @lombok.AllArgsConstructor
  public static class CacheStatistics {
    private boolean enabled;
    private int size;
    private int knownLocations;
  }
}