package ru.yandex.practicum.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.model.hub.*;
import ru.yandex.practicum.kafka.telemetry.event.*;


@Component
public class HubEventMapper {

    public HubEventAvro mapToAvro(HubEvent event) {
        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp());

        switch (event.getType()) {
            case DEVICE_ADDED:
                DeviceAddedEvent addedEvent = (DeviceAddedEvent) event;
                builder.setPayload(DeviceAddedEventAvro.newBuilder()
                        .setId(addedEvent.getId())
                        .setType(DeviceTypeAvro.valueOf(addedEvent.getDeviceType().name()))
                        .build());
                break;

            case DEVICE_REMOVED:
                DeviceRemovedEvent removedEvent = (DeviceRemovedEvent) event;
                builder.setPayload(DeviceRemovedEventAvro.newBuilder()
                        .setId(removedEvent.getId())
                        .build());
                break;

            case SCENARIO_ADDED:
                ScenarioAddedEvent scenarioAdded = (ScenarioAddedEvent) event;
                builder.setPayload(mapScenarioAdded(scenarioAdded));
                break;

            case SCENARIO_REMOVED:
                ScenarioRemovedEvent scenarioRemoved = (ScenarioRemovedEvent) event;
                builder.setPayload(ScenarioRemovedEventAvro.newBuilder()
                        .setName(scenarioRemoved.getName())
                        .build());
                break;

            default:
                throw new IllegalArgumentException("Unknown hub event type: " + event.getType());
        }

        return builder.build();
    }

    private ScenarioAddedEventAvro mapScenarioAdded(ScenarioAddedEvent event) {
        return ScenarioAddedEventAvro.newBuilder()
                .setName(event.getName())
                .setConditions(event.getConditions().stream()
                        .map(this::mapCondition)
                        .toList())
                .setActions(event.getActions().stream()
                        .map(this::mapAction)
                        .toList())
                .build();
    }

    private ScenarioConditionAvro mapCondition(ScenarioCondition condition) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()));

        if (condition.getValue() != null) {
            builder.setValue(condition.getValue());
        } else {
            builder.setValue(null);
        }

        return builder.build();
    }

    private DeviceActionAvro mapAction(DeviceAction action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()));

        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        } else {
            builder.setValue(null);
        }

        return builder.build();
    }
}