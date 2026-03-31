package ru.yandex.practicum.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.deserializer.HubEventDeserializer;
import ru.yandex.practicum.deserializer.SensorsSnapshotDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Properties;

@Configuration
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Общие настройки для снапшотов (высокая интенсивность)
    @Value("${kafka.consumer.snapshot.group-id:analyzer-snapshot-group}")
    private String snapshotGroupId;
    @Value("${kafka.consumer.snapshot.max-poll-records:10}")
    private int snapshotMaxPollRecords;
    @Value("${kafka.consumer.snapshot.enable-auto-commit:false}")
    private boolean snapshotAutoCommit;

    // Настройки для событий хаба (низкая интенсивность)
    @Value("${kafka.consumer.hub.group-id:analyzer-hub-group}")
    private String hubGroupId;
    @Value("${kafka.consumer.hub.max-poll-records:100}")
    private int hubMaxPollRecords;
    @Value("${kafka.consumer.hub.enable-auto-commit:true}")
    private boolean hubAutoCommit;
    @Value("${kafka.consumer.hub.auto-commit-interval:5000}")
    private int hubAutoCommitInterval;

    // Общие настройки
    @Value("${kafka.consumer.fetch-max-bytes:3072000}")
    private int fetchMaxBytes;
    @Value("${kafka.consumer.max-partition-fetch-bytes:307200}")
    private int maxPartitionFetchBytes;

    /**
     * Консьюмер для снапшотов (высокая интенсивность)
     * Повторная обработка крайне нежелательна → ручная фиксация смещений
     * Маленький batch, чтобы быстро обрабатывать и фиксировать
     */
    @Bean("snapshotConsumer")
    public org.apache.kafka.clients.consumer.KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, snapshotGroupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "analyzer-snapshot-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorsSnapshotDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, snapshotMaxPollRecords);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, snapshotAutoCommit);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    }

    /**
     * Консьюмер для событий хаба (низкая интенсивность)
     * Повторная обработка не критична → авто-коммит
     * Большой batch, можно реже опрашивать
     */
    @Bean("hubEventConsumer")
    public org.apache.kafka.clients.consumer.KafkaConsumer<String, HubEventAvro> hubEventConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, hubGroupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "analyzer-hub-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HubEventDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, hubMaxPollRecords);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, hubAutoCommit);  // true — авто-коммит
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, hubAutoCommitInterval);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    }
}
