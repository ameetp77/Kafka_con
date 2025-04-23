package com.example.kafka;

import java.util.Arrays;
import java.util.Properties;

public class Application {
    public static void main(String[] args) {
        // Kafka consumer configuration
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("group.id", "batch-processor-group");
        
        // Configure batch size, number of worker threads, and batch timeout
        int batchSize = 1000; // Number of records to pull from Kafka at once
        int numWorkerThreads = 10; // Number of worker threads
        long batchTimeoutSeconds = 60; // Maximum time allowed for processing a batch
        
        // Create and start the batch processor
        KafkaBatchProcessor processor = new KafkaBatchProcessor(
            kafkaProps, 
            batchSize, 
            numWorkerThreads, 
            batchTimeoutSeconds
        );
        processor.subscribe(Arrays.asList("input-topic"));
        
        // Add shutdown hook to gracefully shutdown the processor
        Runtime.getRuntime().addShutdownHook(new Thread(processor::shutdown));
        
        // Start processing
        processor.start();
    }
}
