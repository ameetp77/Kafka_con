package com.example.kafka;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class WorkerThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(WorkerThread.class);
    private static final int MAX_KINESIS_RECORDS = 500;
    private static final int MAX_KINESIS_BATCH_SIZE_BYTES = 5 * 1024 * 1024; // 5MB (to stay under 1MB/s with some margin)
    
    private final int id;
    private final BlockingQueue<KafkaConsumerService.KafkaMessageWrapper> messageQueue;
    private final AmazonKinesis kinesisClient;
    private final KafkaConsumer<String, String> consumer;
    private final String kinesisStreamName;
    private final MetricsService metricsService;
    private volatile boolean running = true;
    
    public WorkerThread(
            int id, 
            AmazonKinesis kinesisClient, 
            KafkaConsumer<String, String> consumer,
            String kinesisStreamName,
            MetricsService metricsService) {
        this.id = id;
        this.messageQueue = new LinkedBlockingQueue<>();
        this.kinesisClient = kinesisClient;
        this.consumer = consumer;
        this.kinesisStreamName = kinesisStreamName;
        this.metricsService = metricsService;
    }
    
    public void addMessage(KafkaConsumerService.KafkaMessageWrapper message) {
        messageQueue.add(message);
    }
    
    @Override
    public void run() {
        logger.info("Worker thread {} started", id);
        try {
            while (running || !messageQueue.isEmpty()) {
                List<KafkaConsumerService.KafkaMessageWrapper> messageBatch = new ArrayList<>();
                
                // Get at least one message or wait
                KafkaConsumerService.KafkaMessageWrapper message = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (message != null) {
                    messageBatch.add(message);
                    
                    // Try to get more messages up to MAX_KINESIS_RECORDS but don't block
                    messageQueue.drainTo(messageBatch, MAX_KINESIS_RECORDS - 1);
                    
                    // Process the batch
                    processMessages(messageBatch);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Worker thread {} was interrupted", id);
        } catch (Exception e) {
            logger.error("Error in worker thread {}: {}", id, e.getMessage(), e);
            metricsService.incrementErrorCount("WorkerThread-" + id);
        } finally {
            logger.info("Worker thread {} shutting down", id);
        }
    }
    
    private void processMessages(List<KafkaConsumerService.KafkaMessageWrapper> messages) {
        if (messages.isEmpty()) return;
        
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        List<PutRecordsRequestEntry> kinesisRecords = new ArrayList<>();
        int currentBatchSizeBytes = 0;
        
        for (KafkaConsumerService.KafkaMessageWrapper message : messages) {
            long startTime = System.currentTimeMillis();
            
            try {
                // Create Kinesis record
                byte[] data = message.getPayload().getBytes();
                int recordSize = data.length;
                
                // If adding this record would exceed the batch size limit, send the current batch first
                if (kinesisRecords.size() >= MAX_KINESIS_RECORDS || 
                    (currentBatchSizeBytes + recordSize) > MAX_KINESIS_BATCH_SIZE_BYTES) {
                    sendToKinesis(kinesisRecords);
                    kinesisRecords.clear();
                    currentBatchSizeBytes = 0;
                }
                
                // Add the record to the batch
                PutRecordsRequestEntry entry = new PutRecordsRequestEntry()
                    .withData(ByteBuffer.wrap(data))
                    .withPartitionKey(UUID.randomUUID().toString());
                
                kinesisRecords.add(entry);
                currentBatchSizeBytes += recordSize;
                
                // Track the latest offset for each partition to commit
                TopicPartition tp = message.getTopicPartition();
                offsetsToCommit.put(tp, new OffsetAndMetadata(message.getOffset() + 1));
                
                // Record processing time metric
                long processingTime = System.currentTimeMillis() - startTime;
                metricsService.recordProcessingTime("WorkerThread-" + id, processingTime);
                
            } catch (Exception e) {
                logger.error("Error processing message in worker {}: {}", id, e.getMessage(), e);
                metricsService.incrementErrorCount("WorkerThread-" + id);
            }
        }
        
        // Send any remaining records to Kinesis
        if (!kinesisRecords.isEmpty()) {
            sendToKinesis(kinesisRecords);
        }
        
        // Commit offsets for all processed messages
        try {
            if (!offsetsToCommit.isEmpty()) {
                consumer.commitSync(offsetsToCommit);
                logger.debug("Worker {} committed offsets for {} partitions", id, offsetsToCommit.size());
            }
        } catch (CommitFailedException e) {
            logger.error("Failed to commit offsets in worker {}: {}", id, e.getMessage());
            metricsService.incrementErrorCount("WorkerThread-" + id + "-CommitFailure");
        }
    }
    
    private void sendToKinesis(List<PutRecordsRequestEntry> records) {
        if (records.isEmpty()) return;
        
        try {
            long startTime = System.currentTimeMillis();
            
            PutRecordsRequest putRecordsRequest = new PutRecordsRequest()
                .withStreamName(kinesisStreamName)
                .withRecords(records);
            
            kinesisClient.putRecords(putRecordsRequest);
            
            long duration = System.currentTimeMillis() - startTime;
            logger.debug("Worker {} sent {} records to Kinesis in {} ms", id, records.size(), duration);
            
            metricsService.recordKinesisWriteTime("WorkerThread-" + id, duration);
            metricsService.recordKinesisRecordCount("WorkerThread-" + id, records.size());
            
        } catch (Exception e) {
            logger.error("Error sending records to Kinesis from worker {}: {}", id, e.getMessage(), e);
            metricsService.incrementErrorCount("WorkerThread-" + id + "-KinesisWriteFailure");
        }
    }
    
    public void shutdown() {
        running = false;
    }
}
