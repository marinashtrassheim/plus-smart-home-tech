package ru.yandex.practicum.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.service.SnapshotAnalyzerService;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Обработчик снапшотов (основной поток)
 * Использует ручную фиксацию смещений — повторная обработка крайне нежелательна
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer;
    private final SnapshotAnalyzerService snapshotAnalyzerService;

    @Value("${kafka.topic.snapshots}")
    private String snapshotsTopic;

    @Value("${kafka.consumer.snapshot.poll-timeout-ms:1000}")
    private long pollTimeoutMs;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public void start() {
        log.info("Starting SnapshotProcessor...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("SnapshotProcessor shutdown signal received, waking up consumer...");
            snapshotConsumer.wakeup();
        }));

        try {
            snapshotConsumer.subscribe(List.of(snapshotsTopic));
            log.info("SnapshotProcessor subscribed to topic: {}", snapshotsTopic);

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        snapshotConsumer.poll(Duration.ofMillis(pollTimeoutMs));

                int recordCount = records.count();
                if (recordCount > 0) {
                    log.info("SnapshotProcessor received {} records", recordCount);
                }

                int processedCount = 0;
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    processSnapshot(record);
                    trackOffset(record);
                    processedCount++;
                }

                if (!records.isEmpty()) {
                    commitOffsets();
                }
            }

        } catch (WakeupException e) {
            log.info("SnapshotProcessor wakeup called - initiating graceful shutdown");
        } catch (Exception e) {
            log.error("Unexpected error in SnapshotProcessor", e);
        } finally {
            shutdown();
        }
    }

    private void processSnapshot(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        SensorsSnapshotAvro snapshot = record.value();
        if (snapshot == null) {
            log.warn("Received null snapshot, skipping");
            return;
        }

        log.debug("Processing snapshot: hubId={}, timestamp={}, offset={}",
                snapshot.getHubId(), snapshot.getTimestamp(), record.offset());

        try {
            snapshotAnalyzerService.analyzeSnapshot(snapshot);
        } catch (Exception e) {
            log.error("Error processing snapshot: hubId={}, offset={}",
                    snapshot.getHubId(), record.offset(), e);
            throw new RuntimeException("Snapshot processing failed", e);
        }
    }

    private void trackOffset(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        long nextOffset = record.offset() + 1;
        currentOffsets.put(partition, new OffsetAndMetadata(nextOffset));
    }

    private void commitOffsets() {
        if (!currentOffsets.isEmpty()) {
            try {
                snapshotConsumer.commitSync(currentOffsets);
                log.debug("Snapshot offsets committed: {}", currentOffsets);
                currentOffsets.clear();
            } catch (Exception e) {
                log.error("Failed to commit snapshot offsets", e);
                throw new RuntimeException("Offset commit failed", e);
            }
        }
    }

    private void shutdown() {
        log.info("Shutting down SnapshotProcessor...");
        try {
            if (!currentOffsets.isEmpty()) {
                snapshotConsumer.commitSync(currentOffsets);
            }
        } catch (Exception e) {
            log.error("Error during final offset commit", e);
        } finally {
            try {
                snapshotConsumer.close();
            } catch (Exception e) {
                log.error("Error closing snapshot consumer", e);
            }
        }
        log.info("SnapshotProcessor shutdown completed");
    }
}