package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaEventProducer {
    private final KafkaProducer<String, byte[]> producer;
    private final EncoderFactory encoderFactory = EncoderFactory.get();

    public void sendSensorEvent(String topic, SensorEventAvro event) {
        sendEvent(topic, event.getId(), event);
    }

    public void sendHubEvent(String topic, HubEventAvro event) {
        sendEvent(topic, event.getHubId(), event);
    }

    private <T extends SpecificRecordBase> void sendEvent(String topic, String key, T event) {
        try {
            byte[] value = serializeAvro(event);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Error sending event to Kafka: {}", exception.getMessage(), exception);
                } else {
                    log.debug("Event sent to topic {}: partition={}, offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            log.error("Failed to send event to Kafka: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to send event to Kafka", e);
        }
    }

    private <T extends SpecificRecordBase> byte[] serializeAvro(T event) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = encoderFactory.binaryEncoder(out, null);
        DatumWriter<T> writer = new SpecificDatumWriter<>(event.getSchema());

        writer.write(event, encoder);
        encoder.flush();
        out.close();

        return out.toByteArray();
    }
}