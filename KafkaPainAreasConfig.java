package com.company.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * Configuration class for Kafka pain areas solution
 */
@Configuration
public class KafkaPainAreasConfig {

  /**
   * RestTemplate bean for external API calls
   */
  @Bean
  public RestTemplate restTemplate() {
    return new RestTemplate();
  }
}