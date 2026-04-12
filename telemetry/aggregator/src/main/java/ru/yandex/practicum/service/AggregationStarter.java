package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Главный оркестратор агрегации событий датчиков в снапшоты.
 * Реализует паттерн poll loop с ручным управлением смещениями.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final KafkaProducer<String, SensorsSnapshotAvro> producer;
    private final SnapshotAggregator snapshotAggregator;

    @Value("${kafka.topic.sensors}")
    private String sensorsTopic;

    @Value("${kafka.topic.snapshots}")
    private String snapshotsTopic;

    @Value("${kafka.consumer.poll-timeout-ms:1000}")
    private long pollTimeoutMs;

    // Хранилище текущих смещений для ручной фиксации
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    /**
     * Запускает процесс агрегации.
     * Подписывается на топик событий датчиков и обрабатывает записи в poll loop.
     */
    public void start() {
        log.info("Starting AggregationStarter...");

        // Регистрируем хук для graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, waking up consumer...");
            consumer.wakeup();
        }));

        try {
            // Подписываемся на топик с событиями датчиков
            consumer.subscribe(List.of(sensorsTopic));
            log.info("Subscribed to topic: {}", sensorsTopic);

            // Главный poll loop
            while (true) {
                // Опрашиваем записи с таймаутом
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

                int recordCount = records.count();
                if (recordCount > 0) {
                    log.info("Received {} records from Kafka", recordCount);
                }

                // Обрабатываем каждую запись
                int processedCount = 0;
                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    Optional<SensorsSnapshotAvro> updatedSnapshot = processRecord(record);

                    // Если снапшот обновился, отправляем в Kafka
                    if (updatedSnapshot.isPresent()) {
                        sendSnapshot(updatedSnapshot.get());
                    }

                    // Отслеживаем смещения для ручной фиксации
                    trackOffsets(record, processedCount);
                    processedCount++;
                }

                // Фиксируем смещения после обработки батча
                if (!records.isEmpty()) {
                    commitOffsets();
                }
            }

        } catch (WakeupException e) {
            log.info("Consumer wakeup called - initiating graceful shutdown");
        } catch (Exception e) {
            log.error("Unexpected error during aggregation processing", e);
        } finally {
            // Сбрасываем буферы и закрываем ресурсы
            shutdown();
        }
    }

    /**
     * Обрабатывает одну запись события датчика.
     *
     * @param record запись консьюмера с событием датчика
     * @return Optional с обновленным снапшотом, если состояние изменилось, иначе empty
     */
    private Optional<SensorsSnapshotAvro> processRecord(ConsumerRecord<String, SensorEventAvro> record) {
        SensorEventAvro event = record.value();

        if (event == null) {
            log.warn("Received null sensor event, skipping");
            return Optional.empty();
        }

        log.info("Processing sensor event: hubId={}, sensorId={}, timestamp={}",
                event.getHubId(), event.getId(), event.getTimestamp());

        // Обновляем состояние снапшота на основе этого события
        return snapshotAggregator.updateState(event);
    }

    /**
     * Отправляет снапшот в топик Kafka.
     *
     * @param snapshot снапшот для отправки
     */
    private void sendSnapshot(SensorsSnapshotAvro snapshot) {
        try {
            String hubId = snapshot.getHubId();
            ProducerRecord<String, SensorsSnapshotAvro> record =
                    new ProducerRecord<>(snapshotsTopic, hubId, snapshot);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send snapshot for hub {}: {}",
                            hubId, exception.getMessage(), exception);
                } else {
                    log.debug("Snapshot sent for hub {}: partition={}, offset={}",
                            hubId, metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            log.error("Error sending snapshot to Kafka", e);
        }
    }

    /**
     * Отслеживает смещения обработанных записей.
     * Фиксирует асинхронно каждые 10 записей.
     *
     * @param record обработанная запись консьюмера
     * @param index индекс в текущем батче
     */
    private void trackOffsets(ConsumerRecord<String, SensorEventAvro> record, int index) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        long nextOffset = record.offset() + 1;
        currentOffsets.put(partition, new OffsetAndMetadata(nextOffset));

        // Фиксируем асинхронно каждые 10 записей
        if (index % 10 == 0) {
            producer.flush(); // Убеждаемся, что все сообщения отправлены до фиксации смещений
            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                if (exception != null) {
                    log.warn("Async offset commit failed for offsets: {}", offsets, exception);
                } else {
                    log.debug("Async offset commit successful: {}", offsets);
                }
            });
        }
    }

    /**
     * Фиксирует смещения синхронно после обработки батча.
     */
    private void commitOffsets() {
        if (!currentOffsets.isEmpty()) {
            try {
                producer.flush(); // Убеждаемся, что все сообщения отправлены
                consumer.commitSync(currentOffsets);
                log.debug("Sync offset commit successful: {}", currentOffsets);
                currentOffsets.clear();
            } catch (Exception e) {
                log.error("Failed to commit offsets synchronously", e);
            }
        }
    }

    /**
     * Graceful shutdown: сбрасывает продюсер, фиксирует смещения, закрывает ресурсы.
     */
    private void shutdown() {
        log.info("Starting graceful shutdown...");

        try {
            // Сбрасываем все ожидающие сообщения из буфера продюсера
            log.info("Flushing producer...");
            producer.flush();

            // Финальная фиксация оставшихся смещений
            if (!currentOffsets.isEmpty()) {
                log.info("Final offset commit...");
                consumer.commitSync(currentOffsets);
                currentOffsets.clear();
            }
        } catch (Exception e) {
            log.error("Error during final offset commit", e);
        } finally {
            // Закрываем консьюмер и продюсер
            try {
                log.info("Closing consumer...");
                consumer.close();
            } catch (Exception e) {
                log.error("Error closing consumer", e);
            }

            try {
                log.info("Closing producer...");
                producer.close();
            } catch (Exception e) {
                log.error("Error closing producer", e);
            }

            log.info("Graceful shutdown completed");
        }
    }
}