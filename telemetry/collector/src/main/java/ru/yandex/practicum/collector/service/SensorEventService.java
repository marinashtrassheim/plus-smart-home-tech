package ru.yandex.practicum.collector.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.collector.model.sencor.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Service
@RequiredArgsConstructor
public class SensorEventService {
    private final SensorEventMapper mapper;
    private final KafkaEventProducer producer;

    public void processEvent(SensorEvent event, String topic) {
        log.info("Processing sensor event: {}", event.getId());
        SensorEventAvro avroEvent = mapper.mapToAvro(event);
        producer.sendSensorEvent(topic, avroEvent);
    }
}
