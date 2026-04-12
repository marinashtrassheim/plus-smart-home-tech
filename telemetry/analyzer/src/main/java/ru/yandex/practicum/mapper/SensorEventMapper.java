package ru.yandex.practicum.mapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.model.entity.Sensor;

@Slf4j
@Component
@RequiredArgsConstructor
public class SensorEventMapper {

    public Sensor toEntity(DeviceAddedEventAvro event, String hubId) {
        if (event == null) {
            return null;
        }

        return Sensor.builder()
                .id(event.getId())
                .hubId(hubId)
                .deviceType(event.getType() != null ? event.getType().name() : null)
                .build();
    }
}