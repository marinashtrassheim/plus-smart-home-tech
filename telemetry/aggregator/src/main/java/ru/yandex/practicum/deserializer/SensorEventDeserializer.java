package ru.yandex.practicum.deserializer;

import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

/**
 * Десериализатор для событий датчиков (SensorEventAvro)
 * Просто расширяет базовый класс, указывая конкретный тип и схему
 */
public class SensorEventDeserializer extends BaseAvroDeserializer<SensorEventAvro> {

    /**
     * Конструктор по умолчанию
     * Передает в базовый класс схему класса SensorEventAvro
     */
    public SensorEventDeserializer() {
        // SensorEventAvro.getClassSchema() возвращает схему, сгенерированную Avro-компилятором
        super(SensorEventAvro.getClassSchema());
    }
}