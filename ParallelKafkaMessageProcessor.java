package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.Objects;

@Component
public class ParallelKafkaMessageProcessor {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group.id}")
    private String groupId;

    @Value("${kafka.topic}")
    private String topic;

    @Value("${kafka.max.poll.records:5}")
    private int maxPollRecords;

    @Value("${kafka.worker.threads:5}")
    private int workerThreads;

    private KafkaConsumer<String, String> consumer;
    private ExecutorService executorService;
    private volatile boolean running = true;
    private final Map<Long, Boolean> processedOffsets = new ConcurrentHashMap<>();
    private final AtomicLong lastCommittedOffset = new AtomicLong(-1);
    private Thread consumerThread;

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("max.poll.records", maxPollRecords);

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        executorService = Executors.newFixedThreadPool(workerThreads);

        consumerThread = new Thread(this::consumeMessages);
        consumerThread.start();
    }

    private void consumeMessages() {
        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    processRecords(records);
                }

                commitProcessedOffsets();
            } catch (Exception e) {
                System.err.println("Error in consumer thread: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void processRecords(ConsumerRecords<String, String> records) {
        // Group records by their hash value to process related records on the same thread
        Map<Integer, Map<Long, ConsumerRecord<String, String>>> recordsByHash =
                StreamSupport.stream(records.spliterator(), false)
                        .collect(Collectors.groupingBy(
                                record -> getHashValue(record),
                                Collectors.toMap(
                                        ConsumerRecord::offset,
                                        record -> record
                                )
                        ));

        // Create a future for each hash group
        CompletableFuture<?>[] futures = new CompletableFuture[recordsByHash.size()];
        int i = 0;

        // For each hash group, process all records sequentially on one thread
        for (Map.Entry<Integer, Map<Long, ConsumerRecord<String, String>>> entry : recordsByHash.entrySet()) {
            // Mark all offsets as "in progress" by storing false
            for (Long offset : entry.getValue().keySet()) {
                processedOffsets.put(offset, false);
            }

            // Process all records with the same hash value on one thread
            futures[i++] = CompletableFuture.runAsync(() -> {
                try {
                    // Process records in order of their offsets
                    entry.getValue().entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .forEach(recordEntry -> {
                                try {
                                    processMessage(recordEntry.getValue());
                                    // Mark as successfully processed
                                    processedOffsets.put(recordEntry.getKey(), true);
                                } catch (Exception e) {
                                    System.err.println("Failed to process message at offset " +
                                            recordEntry.getKey() + ": " + e.getMessage());
                                    // We keep the offset marked as false to indicate failure
                                }
                            });
                } catch (Exception e) {
                    System.err.println("Error processing hash group " + entry.getKey() + ": " + e.getMessage());
                }
            }, executorService);
        }

        // Wait for all hash group processing tasks to complete
        CompletableFuture.allOf(futures).join();
    }

    private int getHashValue(ConsumerRecord<String, String> record) {
        // Get hash code based on message key or another attribute
        String key = record.key();
        if (key != null) {
            return key.hashCode() % workerThreads;
        }

        // If no key is available, use some other property for hashing
        // For example, you could use a field from the message value
        // This is just a fallback example that uses the first character of the value
        String value = record.value();
        if (value != null && !value.isEmpty()) {
            return value.charAt(0) % workerThreads;
        }

        // Default fallback - not ideal for production
        return (int)(record.offset() % workerThreads);
    }

    private void processMessage(ConsumerRecord<String, String> record) {
        // Actual business logic for processing the message
        System.out.println("Processing message: " + record.value() +
                " at offset: " + record.offset() +
                " on thread: " + Thread.currentThread().getName());

        // Simulate occasional failures
        if (Math.random() < 0.1) { // 10% chance of failure
            throw new RuntimeException("Simulated processing failure");
        }

        // Add your message processing logic here
    }

    private void commitProcessedOffsets() {
        long currentOffset = lastCommittedOffset.get();
        boolean canCommit = true;

        // Find the highest consecutive successfully processed offset
        while (canCommit) {
            Boolean isProcessed = processedOffsets.get(currentOffset + 1);
            if (Boolean.TRUE.equals(isProcessed)) {
                currentOffset++;
            } else {
                // We found a gap or a failed message
                canCommit = false;
            }
        }

        // If we found offsets to commit
        if (currentOffset > lastCommittedOffset.get()) {
            // Commit the new offset (which is the next one to be consumed)
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsetsToCommit =
                    Collections.singletonMap(
                            new TopicPartition(topic, 0),
                            new org.apache.kafka.clients.consumer.OffsetAndMetadata(currentOffset + 1)
                    );

            consumer.commitSync(offsetsToCommit);

            // Update our tracking and clean up processed offsets map
            long oldOffset = lastCommittedOffset.getAndSet(currentOffset);
            for (long i = oldOffset + 1; i <= currentOffset; i++) {
                processedOffsets.remove(i);
            }

            System.out.println("Committed offset: " + (currentOffset + 1));
        }
    }

    @PreDestroy
    public void shutdown() {
        running = false;

        if (consumerThread != null) {
            try {
                consumerThread.join(5000); // Wait up to 5 seconds for the consumer thread to finish
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (consumer != null) {
            consumer.close();
        }

        if (executorService != null) {
            executorService.shutdown();
        }
    }
}