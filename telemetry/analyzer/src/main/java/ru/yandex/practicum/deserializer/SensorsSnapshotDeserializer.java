package ru.yandex.practicum.deserializer;

import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

/**
 * Десериализатор для снапшотов (SensorsSnapshotAvro)
 */
public class SensorsSnapshotDeserializer extends BaseAvroDeserializer<SensorsSnapshotAvro> {

    public SensorsSnapshotDeserializer() {
        super(SensorsSnapshotAvro.getClassSchema());
    }
}