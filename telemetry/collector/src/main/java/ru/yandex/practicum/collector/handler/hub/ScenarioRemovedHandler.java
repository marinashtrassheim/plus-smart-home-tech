package ru.yandex.practicum.collector.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.mapper.HubEventProtoMapper;
import ru.yandex.practicum.collector.service.KafkaEventProducer;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Slf4j
@RequiredArgsConstructor
@Component
public class ScenarioRemovedHandler implements HubEventHandler {

    private final HubEventProtoMapper hubEventProtoMapper;
    private final KafkaEventProducer kafkaEventProducer;
    @Value("${kafka.topic.hubs}")
    private String hubEventsTopic;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_REMOVED;
    }

    @Override
    public void handle(HubEventProto hubEventProto) {
        log.info("Processing hub sensor event: hub_id={}", hubEventProto.getHubId());

        HubEventAvro avroEvent = hubEventProtoMapper.mapToAvro(hubEventProto);
        kafkaEventProducer.sendHubEvent(hubEventsTopic, avroEvent);

        log.info("Motion hub event sent to Kafka: hub_id={}", hubEventProto.getHubId());
    }
}
