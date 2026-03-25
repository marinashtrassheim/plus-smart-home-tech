package ru.yandex.practicum.collector.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
@Configuration
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    @ConditionalOnProperty(name = "kafka.bootstrap-servers", havingValue = "", matchIfMissing = false)
    public KafkaProducer<String, byte[]> kafkaProducer() {
        log.info("Creating real KafkaProducer with bootstrap servers: {}", bootstrapServers);

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return new KafkaProducer<>(config);
    }

    @Bean
    @ConditionalOnProperty(name = "kafka.bootstrap-servers", havingValue = "", matchIfMissing = true)
    public KafkaProducer<String, byte[]> kafkaProducerMock() {
        log.warn("Kafka bootstrap servers is empty. Creating MOCK KafkaProducer (messages will be logged, not sent)");

        return new KafkaProducer<>(new Properties()) {
            @Override
            public java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                    org.apache.kafka.clients.producer.ProducerRecord<String, byte[]> record,
                    org.apache.kafka.clients.producer.Callback callback) {
                log.info("MOCK: Would send to topic '{}' with key '{}'",
                        record.topic(), record.key());
                if (callback != null) {
                    callback.onCompletion(null, null);
                }
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }

            @Override
            public void close() {
                log.info("MOCK: Closing KafkaProducer");
            }

            @Override
            public void close(java.time.Duration timeout) {
                log.info("MOCK: Closing KafkaProducer with timeout");
            }
        };
    }
}