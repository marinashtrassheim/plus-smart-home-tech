package ru.yandex.practicum.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.deserializer.SensorEventDeserializer;
import ru.yandex.practicum.serializer.SensorsSnapshotSerializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Properties;

@Configuration
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${kafka.consumer.max-poll-records:100}")
    private int maxPollRecords;

    @Value("${kafka.consumer.fetch-max-bytes:3072000}")
    private int fetchMaxBytes;

    @Value("${kafka.consumer.max-partition-fetch-bytes:307200}")
    private int maxPartitionFetchBytes;

    @Value("${kafka.producer.acks:1}")
    private String acks;

    @Value("${kafka.producer.retries:3}")
    private int retries;

    @Value("${kafka.producer.batch-size:16384}")
    private int batchSize;

    @Value("${kafka.producer.linger-ms:10}")
    private int lingerMs;

    @Value("${kafka.producer.buffer-memory:33554432}")
    private long bufferMemory;

    @Value("${kafka.producer.compression-type:snappy}")
    private String compressionType;

    /**
     * Консьюмер для чтения событий от датчиков (SensorEventAvro)
     */
    @Bean
    public org.apache.kafka.clients.consumer.KafkaConsumer<String, SensorEventAvro> kafkaConsumer() {
        Properties props = new Properties();

        // Общие настройки
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "aggregator-consumer");

        // Сериализаторы/десериализаторы
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventDeserializer.class.getName());

        // Настройки потребления
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);

        // Автокоммит отключаем, будем управлять смещениями вручную
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Стратегия назначения партиций
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.RoundRobinAssignor");

        // Начало чтения: с самого начала, если нет зафиксированного смещения
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    }

    /**
     * Продюсер для отправки снапшотов (SensorsSnapshotAvro)
     */
    @Bean
    public org.apache.kafka.clients.producer.KafkaProducer<String, SensorsSnapshotAvro> kafkaProducer() {
        Properties props = new Properties();

        // Общие настройки
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "aggregator-producer");

        // Сериализаторы
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SensorsSnapshotSerializer.class.getName());

        // Настройки надежности
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);

        // Настройки производительности
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);

        // Идемпотентность (гарантирует отсутствие дубликатов)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }
}