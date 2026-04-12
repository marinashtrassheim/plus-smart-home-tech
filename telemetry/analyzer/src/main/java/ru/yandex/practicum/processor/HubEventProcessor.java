package ru.yandex.practicum.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.service.HubEventService;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.List;

/**
 * Обработчик событий от хаба (добавление/удаление устройств и сценариев)
 * Работает в отдельном потоке, использует авто-коммит (повторная обработка не критична)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, HubEventAvro> hubEventConsumer;
    private final HubEventService hubEventService;

    @Value("${kafka.topic.hubs}")
    private String hubsTopic;

    @Value("${kafka.consumer.hub.poll-timeout-ms:1000}")
    private long pollTimeoutMs;

    private volatile boolean running = true;

    @Override
    public void run() {
        log.info("Starting HubEventProcessor...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("HubEventProcessor shutdown signal received");
            running = false;
            hubEventConsumer.wakeup();
        }));

        try {
            hubEventConsumer.subscribe(List.of(hubsTopic));
            log.info("HubEventProcessor subscribed to topic: {}", hubsTopic);

            while (running) {
                ConsumerRecords<String, HubEventAvro> records = hubEventConsumer.poll(Duration.ofMillis(pollTimeoutMs));

                int recordCount = records.count();
                if (recordCount > 0) {
                    log.debug("HubEventProcessor received {} records", recordCount);
                }

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    processRecord(record);
                }
            }

        } catch (WakeupException e) {
            log.info("HubEventProcessor wakeup called");
        } catch (Exception e) {
            log.error("Unexpected error in HubEventProcessor", e);
        } finally {
            shutdown();
        }
    }

    private void processRecord(ConsumerRecord<String, HubEventAvro> record) {
        HubEventAvro event = record.value();
        if (event == null) {
            log.warn("Received null hub event, skipping");
            return;
        }

        log.debug("Processing hub event: hubId={}, timestamp={}, offset={}",
                event.getHubId(), event.getTimestamp(), record.offset());

        try {
            hubEventService.processHubEvent(event);
        } catch (Exception e) {
            log.error("Error processing hub event: hubId={}, offset={}",
                    event.getHubId(), record.offset(), e);
            // При ошибке не бросаем исключение, чтобы не прерывать poll loop
            // Авто-коммит все равно зафиксирует offset, что допустимо для событий хаба
        }
    }

    private void shutdown() {
        log.info("Shutting down HubEventProcessor...");
        try {
            hubEventConsumer.wakeup();
            hubEventConsumer.close();
        } catch (Exception e) {
            log.error("Error closing hub event consumer", e);
        }
        log.info("HubEventProcessor shutdown completed");
    }
}