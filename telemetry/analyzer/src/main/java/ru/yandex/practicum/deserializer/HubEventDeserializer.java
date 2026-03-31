package ru.yandex.practicum.deserializer;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

/**
 * Десериализатор для событий хаба (HubEventAvro)
 */
public class HubEventDeserializer extends BaseAvroDeserializer<HubEventAvro> {

    public HubEventDeserializer() {
        super(HubEventAvro.getClassSchema());
    }
}