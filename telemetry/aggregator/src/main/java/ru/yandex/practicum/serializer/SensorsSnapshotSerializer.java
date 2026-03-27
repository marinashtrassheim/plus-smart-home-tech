package ru.yandex.practicum.serializer;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Сериализатор для снапшотов (SensorsSnapshotAvro).
 * Преобразует объект SensorsSnapshotAvro в массив байтов для отправки в Kafka.
 * Использует стандартный подход Avro сериализации.
 */
public class SensorsSnapshotSerializer implements Serializer<SensorsSnapshotAvro> {

    private final EncoderFactory encoderFactory = EncoderFactory.get();

    @Override
    public byte[] serialize(String topic, SensorsSnapshotAvro snapshot) {
        if (snapshot == null) {
            return null;
        }

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            // Создаем бинарный энкодер
            BinaryEncoder encoder = encoderFactory.binaryEncoder(outputStream, null);

            // Создаем writer для записи Avro-объекта
            DatumWriter<SensorsSnapshotAvro> writer = new SpecificDatumWriter<>(SensorsSnapshotAvro.class);

            // Сериализуем объект
            writer.write(snapshot, encoder);

            // Сбрасываем все данные из буфера в поток
            encoder.flush();

            // Возвращаем сериализованные байты
            return outputStream.toByteArray();

        } catch (IOException e) {
            throw new SerializationException("Serialization error: " + topic, e);
        }
    }

    @Override
    public void close() {
        // Ничего не нужно закрывать, так как ресурсы не создавались
    }
}