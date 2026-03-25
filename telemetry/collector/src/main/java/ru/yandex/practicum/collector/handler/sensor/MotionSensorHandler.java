package ru.yandex.practicum.collector.handler.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.mapper.SensorEventProtoMapper;
import ru.yandex.practicum.collector.service.KafkaEventProducer;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Component
@RequiredArgsConstructor
public class MotionSensorHandler implements SensorEventHandler {

    private final SensorEventProtoMapper protoMapper;
    private final KafkaEventProducer kafkaEventProducer;
    @Value("${kafka.topic.sensors}")
    private String sensorEventsTopic;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        log.info("Processing motion sensor event: id={}", event.getId());

        SensorEventAvro avroEvent = protoMapper.mapToAvro(event);
        kafkaEventProducer.sendSensorEvent(sensorEventsTopic, avroEvent);

        log.info("Motion sensor event sent to Kafka: id={}", event.getId());
    }
}