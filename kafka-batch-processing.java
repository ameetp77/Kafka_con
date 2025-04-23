package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaBatchProcessor {
    private static final Logger logger = LoggerFactory.getLogger(KafkaBatchProcessor.class);
    
    private final KafkaConsumer<String, String> consumer;
    private final int batchSize;
    private final int numWorkerThreads;
    private final long batchTimeoutMillis;
    private final ExecutorService executorService;
    private final List<WorkerThread> workers;
    private final ConcurrentMap<BatchId, BatchTracker> batchTrackers;
    private final ScheduledExecutorService timeoutExecutor;
    private volatile boolean running = true;

    public KafkaBatchProcessor(Properties kafkaProps, int batchSize, int numWorkerThreads, long batchTimeoutSeconds) {
        this.batchSize = batchSize;
        this.numWorkerThreads = numWorkerThreads;
        this.batchTimeoutMillis = batchTimeoutSeconds * 1000;
        
        // Configure Kafka consumer
        Properties props = new Properties();
        props.putAll(kafkaProps);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Manual commit
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(batchSize));
        
        this.consumer = new KafkaConsumer<>(props);
        this.executorService = Executors.newFixedThreadPool(numWorkerThreads);
        this.timeoutExecutor = Executors.newScheduledThreadPool(1);
        this.workers = new ArrayList<>(numWorkerThreads);
        this.batchTrackers = new ConcurrentHashMap<>();
        
        // Initialize worker threads
        for (int i = 0; i < numWorkerThreads; i++) {
            WorkerThread worker = new WorkerThread(i);
            workers.add(worker);
            executorService.submit(worker);
        }
    }
    
    public void subscribe(List<String> topics) {
        consumer.subscribe(topics);
    }
    
    public void start() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                if (!records.isEmpty()) {
                    processBatch(records);
                }
            }
        } finally {
            shutdown();
        }
    }
    
    private void processBatch(ConsumerRecords<String, String> records) {
        BatchId batchId = new BatchId(UUID.randomUUID().toString());
        int recordCount = 0;
        
        // Create a batch tracker for this batch
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            recordCount += partitionRecords.size();
            
            // Store the last offset for each partition
            if (!partitionRecords.isEmpty()) {
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                offsetsToCommit.put(partition, new OffsetAndMetadata(lastOffset + 1));
            }
        }
        
        BatchTracker batchTracker = new BatchTracker(batchId, recordCount, offsetsToCommit);
        batchTrackers.put(batchId, batchTracker);
        
        // Set up timeout for this batch
        scheduleTimeoutForBatch(batchId);
        
        // Distribute messages to worker threads based on hash
        for (ConsumerRecord<String, String> record : records) {
            String key = record.key() != null ? record.key() : UUID.randomUUID().toString();
            int hash = Math.abs(key.hashCode() % numWorkerThreads);
            WorkerThread worker = workers.get(hash);
            worker.addMessage(new KafkaMessage(record.value(), batchId));
        }
    }
    
    private void scheduleTimeoutForBatch(BatchId batchId) {
        timeoutExecutor.schedule(() -> {
            BatchTracker tracker = batchTrackers.get(batchId);
            if (tracker != null && !tracker.isCompleted()) {
                handleBatchTimeout(tracker);
            }
        }, batchTimeoutMillis, TimeUnit.MILLISECONDS);
    }
    
    private void handleBatchTimeout(BatchTracker tracker) {
        logger.warn("Batch {} timed out after {} ms with {} messages still unprocessed", 
                tracker.getBatchId(), batchTimeoutMillis, tracker.getRemainingMessages());
        
        // Remove the batch tracker
        batchTrackers.remove(tracker.getBatchId());
        
        // Handle incomplete batch - we can either:
        // 1. Commit anyway (loss of data possible)
        // 2. Not commit (possible duplicate processing)
        // Here we choose option 2 - we don't commit and let Kafka rebalance handle it
        
        // Optionally, we could move unprocessed messages to a dead letter queue
        if (tracker.getRemainingMessages() > 0) {
            // Example: publish to a dead letter topic or log for later recovery
            logger.error("Some messages in batch {} were not processed before timeout", tracker.getBatchId());
        }
    }
    
    public void shutdown() {
        running = false;
        
        // Shutdown timeout executor
        timeoutExecutor.shutdown();
        try {
            if (!timeoutExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                timeoutExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            timeoutExecutor.shutdownNow();
        }
        
        // Shutdown workers
        workers.forEach(WorkerThread::shutdown);
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Close Kafka consumer
        consumer.close();
    }
    
    // Called by worker threads when they complete processing a message
    void messageProcessed(BatchId batchId) {
        BatchTracker tracker = batchTrackers.get(batchId);
        if (tracker != null) {
            if (tracker.messageCompleted()) {
                // All messages in the batch are processed, commit offsets
                try {
                    consumer.commitSync(tracker.getOffsetsToCommit());
                    logger.info("Successfully committed offsets for batch {}", batchId);
                } catch (CommitFailedException e) {
                    logger.error("Failed to commit offsets for batch {}: {}", batchId, e.getMessage());
                    // Handle commit failure - could retry or add to a dead letter queue
                } finally {
                    batchTrackers.remove(batchId);
                }
            }
        }
    }
    
    // Class to track batch processing status
    private static class BatchTracker {
        private final BatchId batchId;
        private final AtomicInteger remainingMessages;
        private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;
        private volatile boolean completed = false;
        
        public BatchTracker(BatchId batchId, int totalMessages, Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
            this.batchId = batchId;
            this.remainingMessages = new AtomicInteger(totalMessages);
            this.offsetsToCommit = offsetsToCommit;
        }
        
        public boolean messageCompleted() {
            int remaining = remainingMessages.decrementAndGet();
            if (remaining == 0) {
                completed = true;
                return true;
            }
            return false;
        }
        
        public boolean isCompleted() {
            return completed;
        }
        
        public int getRemainingMessages() {
            return remainingMessages.get();
        }
        
        public BatchId getBatchId() {
            return batchId;
        }
        
        public Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit() {
            return offsetsToCommit;
        }
    }

    // Worker thread implementation
    private class WorkerThread implements Runnable {
        private final int id;
        private final BlockingQueue<KafkaMessage> messageQueue;
        private final KinesisProducer kinesisProducer;
        private volatile boolean running = true;
        
        public WorkerThread(int id) {
            this.id = id;
            this.messageQueue = new LinkedBlockingQueue<>();
            this.kinesisProducer = new KinesisProducer();
        }
        
        public void addMessage(KafkaMessage message) {
            messageQueue.add(message);
        }
        
        @Override
        public void run() {
            try {
                while (running || !messageQueue.isEmpty()) {
                    List<KafkaMessage> messageBatch = new ArrayList<>(500); // Max 500 messages at a time
                    
                    // Get at least one message or wait
                    KafkaMessage message = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        messageBatch.add(message);
                        
                        // Try to get more messages up to 500 but don't block
                        messageQueue.drainTo(messageBatch, 499); // Already have 1, so get up to 499 more
                        
                        // Process the batch
                        processBatch(messageBatch);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("Error in worker thread {}: {}", id, e.getMessage(), e);
            } finally {
                kinesisProducer.close();
            }
        }
        
        private void processBatch(List<KafkaMessage> messages) {
            try {
                // Send messages to Kinesis in batches up to 500
                kinesisProducer.sendBatch(messages);
                
                // Mark each message as processed
                for (KafkaMessage message : messages) {
                    messageProcessed(message.getBatchId());
                }
            } catch (Exception e) {
                // Log error but don't mark messages as processed
                logger.error("Failed to process batch in worker {}: {}", id, e.getMessage(), e);
                // Optionally add retry logic here or move to dead letter queue
            }
        }
        
        public void shutdown() {
            running = false;
        }
    }
    
    // Simple class representing a message with batch tracking
    public static class KafkaMessage {
        private final String payload;
        private final BatchId batchId;
        
        public KafkaMessage(String payload, BatchId batchId) {
            this.payload = payload;
            this.batchId = batchId;
        }
        
        public String getPayload() {
            return payload;
        }
        
        public BatchId getBatchId() {
            return batchId;
        }
    }
    
    // Identifier for a batch
    public static class BatchId {
        private final String id;
        
        public BatchId(String id) {
            this.id = id;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BatchId batchId = (BatchId) o;
            return Objects.equals(id, batchId.id);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
        
        @Override
        public String toString() {
            return id;
        }
    }
}
