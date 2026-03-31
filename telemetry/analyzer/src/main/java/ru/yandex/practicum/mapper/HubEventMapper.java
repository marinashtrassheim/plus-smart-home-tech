package ru.yandex.practicum.mapper;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.entity.Scenario;
import ru.yandex.practicum.model.entity.Sensor;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventMapper {
    private final SensorEventMapper sensorMapper;
    private final ScenarioEventMapper scenarioMapper;

    public Optional<ProcessedEvent> process(HubEventAvro event) {
        if (event == null) {
            return Optional.empty();
        }

        String hubId = event.getHubId();
        Object payload = event.getPayload();

        if (payload == null) {
            log.warn("Hub event payload is null");
            return Optional.empty();
        }

        switch (payload) {
            case DeviceAddedEventAvro deviceAddedEventAvro -> {
                return processDeviceAdded(hubId, deviceAddedEventAvro);
            }
            case DeviceRemovedEventAvro deviceRemovedEventAvro -> {
                return processDeviceRemoved(hubId, deviceRemovedEventAvro);
            }
            case ScenarioAddedEventAvro scenarioAddedEventAvro -> {
                return processScenarioAdded(hubId, scenarioAddedEventAvro);
            }
            case ScenarioRemovedEventAvro scenarioRemovedEventAvro -> {
                return processScenarioRemoved(hubId, scenarioRemovedEventAvro);
            }
            default -> {
                log.warn("Unknown hub event payload type: {}", payload.getClass().getSimpleName());
                return Optional.empty();
            }
        }
    }

    private Optional<ProcessedEvent> processDeviceAdded(String hubId, DeviceAddedEventAvro payload) {
        Sensor sensor = sensorMapper.toEntity(payload, hubId);
        return Optional.of(new ProcessedEvent(EventType.DEVICE_ADDED, sensor));
    }

    private Optional<ProcessedEvent> processDeviceRemoved(String hubId, DeviceRemovedEventAvro payload) {
        String sensorId = payload.getId();  // напрямую из события
        return Optional.of(new ProcessedEvent(EventType.DEVICE_REMOVED, sensorId, hubId));
    }

    private Optional<ProcessedEvent> processScenarioAdded(String hubId, ScenarioAddedEventAvro payload) {
        Scenario scenario = scenarioMapper.toEntity(payload, hubId);
        return Optional.of(new ProcessedEvent(EventType.SCENARIO_ADDED, scenario));
    }

    private Optional<ProcessedEvent> processScenarioRemoved(String hubId, ScenarioRemovedEventAvro payload) {
        String scenarioName = payload != null ? payload.getName() : null;
        return Optional.of(new ProcessedEvent(EventType.SCENARIO_REMOVED, scenarioName, hubId));
    }

    @Getter
    public static class ProcessedEvent {
        private final EventType type;
        private final Object data;
        private final String hubId;

        public ProcessedEvent(EventType type, Object data) {
            this(type, data, null);
        }

        public ProcessedEvent(EventType type, Object data, String hubId) {
            this.type = type;
            this.data = data;
            this.hubId = hubId;
        }
    }

    public enum EventType {
        DEVICE_ADDED,
        DEVICE_REMOVED,
        SCENARIO_ADDED,
        SCENARIO_REMOVED
    }
}