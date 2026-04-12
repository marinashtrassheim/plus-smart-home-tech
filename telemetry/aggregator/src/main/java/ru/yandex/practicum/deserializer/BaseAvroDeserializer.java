package ru.yandex.practicum.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

/**
 * Базовый десериализатор для Avro-сообщений.
 * Работает с любыми классами, сгенерированными из Avro-схем.
 *
 * @param <T> тип объекта, который нужно десериализовать (должен наследовать SpecificRecordBase)
 */
@Slf4j
public abstract class BaseAvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

    private final DecoderFactory decoderFactory;
    private final Schema schema;
    private final DatumReader<T> reader;

    /**
     * Конструктор с указанием схемы
     * Использует DecoderFactory по умолчанию
     *
     * @param schema схема Avro для десериализации
     */
    public BaseAvroDeserializer(Schema schema) {
        this(DecoderFactory.get(), schema);
    }

    /**
     * Основной конструктор с возможностью указать свою фабрику декодеров
     *
     * @param decoderFactory фабрика для создания декодеров
     * @param schema схема Avro для десериализации
     */
    public BaseAvroDeserializer(DecoderFactory decoderFactory, Schema schema) {
        this.decoderFactory = decoderFactory;
        this.schema = schema;
        this.reader = new SpecificDatumReader<>(schema);
    }

    /**
     * Десериализует массив байтов в объект типа T
     *
     * @param topic название топика (может пригодиться для логирования)
     * @param data массив байтов для десериализации
     * @return десериализованный объект или null, если data == null
     * @throws SerializationException если произошла ошибка десериализации
     */
    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        log.debug("Deserializing {} bytes from topic: {}", data.length, topic);
        try {
            // Создаем декодер для чтения байтов
            Decoder decoder = decoderFactory.binaryDecoder(data, null);

            // Читаем данные и преобразуем в объект
            return reader.read(null, decoder);

        } catch (IOException e) {
            throw new SerializationException("Deserialization error: " + topic, e);
        }
    }

    @Override
    public void close() {
        // Ничего не нужно закрывать, так как ресурсы не создавались
    }
}