package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;

@Service
public class KafkaConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    
    @Value("${kafka.consumer.batch-size}")
    private int batchSize;
    
    @Value("${kafka.consumer.num-worker-threads}")
    private int numWorkerThreads;
    
    @Value("${kafka.topics}")
    private String topicsStr;
    
    @Autowired
    private ConsumerFactory<String, String> consumerFactory;
    
    @Autowired
    private MetricsService metricsService;
    
    @Autowired
    private WorkerThreadFactory workerThreadFactory;
    
    private KafkaConsumer<String, String> consumer;
    private ExecutorService executorService;
    private List<WorkerThread> workers;
    private volatile boolean running = true;
    
    @PostConstruct
    public void init() {
        // Initialize the consumer
        consumer = consumerFactory.createConsumer();
        
        // Subscribe to topics
        List<String> topics = Arrays.asList(topicsStr.split(","));
        consumer.subscribe(topics);
        
        // Initialize worker threads
        workers = new ArrayList<>(numWorkerThreads);
        executorService = Executors.newFixedThreadPool(numWorkerThreads);
        
        for (int i = 0; i < numWorkerThreads; i++) {
            WorkerThread worker = workerThreadFactory.createWorkerThread(i);
            workers.add(worker);
            executorService.submit(worker);
        }
        
        // Start the consumer thread
        new Thread(this::consumeMessages).start();
    }
    
    private void consumeMessages() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                if (!records.isEmpty()) {
                    distributeRecords(records);
                }
            }
        } catch (Exception e) {
            logger.error("Error in Kafka consumer thread: {}", e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }
    
    private void distributeRecords(ConsumerRecords<String, String> records) {
        // Distribute messages to worker threads based on hash
        for (ConsumerRecord<String, String> record : records) {
            String key = record.key() != null ? record.key() : UUID.randomUUID().toString();
            int hash = Math.abs(key.hashCode() % numWorkerThreads);
            WorkerThread worker = workers.get(hash);
            
            // Create a message wrapper with the record and offset information
            KafkaMessageWrapper message = new KafkaMessageWrapper(
                record.value(),
                new TopicPartition(record.topic(), record.partition()),
                record.offset()
            );
            
            worker.addMessage(message);
        }
    }
    
    @PreDestroy
    public void shutdown() {
        running = false;
        
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
    }
    
    // Simple class representing a message with offset tracking for individual commits
    public static class KafkaMessageWrapper {
        private final String payload;
        private final TopicPartition topicPartition;
        private final long offset;
        
        public KafkaMessageWrapper(String payload, TopicPartition topicPartition, long offset) {
            this.payload = payload;
            this.topicPartition = topicPartition;
            this.offset = offset;
        }
        
        public String getPayload() {
            return payload;
        }
        
        public TopicPartition getTopicPartition() {
            return topicPartition;
        }
        
        public long getOffset() {
            return offset;
        }
    }
}
