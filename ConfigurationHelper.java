package com.example.kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Arrays;

public class ConfigurationHelper {
    
    /**
     * Loads configuration from a properties file
     * 
     * @param configPath Path to the configuration file
     * @return Properties object containing the configuration
     */
    public static Properties loadConfig(String configPath) throws IOException {
        Properties props = new Properties();
        try (InputStream inputStream = new FileInputStream(configPath)) {
            props.load(inputStream);
        }
        return props;
    }
    
    /**
     * Creates a KafkaBatchProcessor with configuration from properties
     * 
     * @param configPath Path to the configuration file
     * @return Configured KafkaBatchProcessor
     */
    public static KafkaBatchProcessor createProcessor(String configPath) throws IOException {
        Properties config = loadConfig(configPath);
        
        // Extract Kafka specific properties
        Properties kafkaProps = new Properties();
        for (String key : config.stringPropertyNames()) {
            if (key.startsWith("kafka.")) {
                kafkaProps.put(key.substring(6), config.getProperty(key));
            }
        }
        
        // Get batch size, thread count, and timeout from config
        int batchSize = Integer.parseInt(config.getProperty("processor.batchSize", "1000"));
        int numWorkerThreads = Integer.parseInt(config.getProperty("processor.numWorkerThreads", "10"));
        long batchTimeoutSeconds = Long.parseLong(config.getProperty("processor.batchTimeoutSeconds", "60"));
        
        // Create the processor
        KafkaBatchProcessor processor = new KafkaBatchProcessor(
            kafkaProps, 
            batchSize, 
            numWorkerThreads,
            batchTimeoutSeconds
        );
        
        // Subscribe to topics
        String topicsStr = config.getProperty("processor.topics");
        if (topicsStr != null && !topicsStr.isEmpty()) {
            String[] topics = topicsStr.split(",");
            processor.subscribe(Arrays.asList(topics));
        }
        
        return processor;
    }
}
