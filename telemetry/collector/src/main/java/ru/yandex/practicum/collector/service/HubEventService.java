package ru.yandex.practicum.collector.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.collector.model.hub.HubEvent;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;


@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventService {
    private final HubEventMapper mapper;
    private final KafkaEventProducer producer;

    public void processEvent(HubEvent event, String topic) {
        log.info("Processing hub event: {}", event.getHubId());
        HubEventAvro avroEvent = mapper.mapToAvro(event);
        producer.sendHubEvent(topic, avroEvent);
    }
}
