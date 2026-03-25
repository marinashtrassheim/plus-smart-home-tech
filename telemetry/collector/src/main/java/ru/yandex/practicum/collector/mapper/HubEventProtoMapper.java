package ru.yandex.practicum.collector.mapper;

import com.google.protobuf.Timestamp;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.stream.Collectors;

@Component
public class HubEventProtoMapper {

    public HubEventAvro mapToAvro(HubEventProto proto) {
        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(proto.getHubId())
                .setTimestamp(convertTimestamp(proto.getTimestamp()));

        switch (proto.getPayloadCase()) {
            case DEVICE_ADDED:
                DeviceAddedEventProto deviceAddedProto = proto.getDeviceAdded();
                builder.setPayload(DeviceAddedEventAvro.newBuilder()
                        .setId(deviceAddedProto.getId())
                        .setType(mapDeviceType(deviceAddedProto.getType()))
                        .build());
                break;

            case DEVICE_REMOVED:
                DeviceRemovedEventProto deviceRemovedProto = proto.getDeviceRemoved();
                builder.setPayload(DeviceRemovedEventAvro.newBuilder()
                        .setId(deviceRemovedProto.getId())
                        .build());
                break;

            case SCENARIO_ADDED:
                ScenarioAddedEventProto scenarioAddedProto = proto.getScenarioAdded();
                builder.setPayload(ScenarioAddedEventAvro.newBuilder()
                        .setName(scenarioAddedProto.getName())
                        .setConditions(scenarioAddedProto.getConditionList().stream()
                                .map(this::mapScenarioCondition)
                                .collect(Collectors.toList()))
                        .setActions(scenarioAddedProto.getActionList().stream()
                                .map(this::mapDeviceAction)
                                .collect(Collectors.toList()))
                        .build());
                break;

            case SCENARIO_REMOVED:
                ScenarioRemovedEventProto scenarioRemovedProto = proto.getScenarioRemoved();
                builder.setPayload(ScenarioRemovedEventAvro.newBuilder()
                        .setName(scenarioRemovedProto.getName())
                        .build());
                break;

            default:
                throw new IllegalArgumentException("Unknown hub event type: " + proto.getPayloadCase());
        }

        return builder.build();
    }

    private DeviceTypeAvro mapDeviceType(DeviceTypeProto proto) {
        return switch (proto) {
            case MOTION_SENSOR -> DeviceTypeAvro.MOTION_SENSOR;
            case TEMPERATURE_SENSOR -> DeviceTypeAvro.TEMPERATURE_SENSOR;
            case LIGHT_SENSOR -> DeviceTypeAvro.LIGHT_SENSOR;
            case CLIMATE_SENSOR -> DeviceTypeAvro.CLIMATE_SENSOR;
            case SWITCH_SENSOR -> DeviceTypeAvro.SWITCH_SENSOR;
            default -> throw new IllegalArgumentException("Unknown device type: " + proto);
        };
    }

    private ScenarioConditionAvro mapScenarioCondition(ScenarioConditionProto proto) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(proto.getSensorId())
                .setType(mapConditionType(proto.getType()))
                .setOperation(mapConditionOperation(proto.getOperation()));

        // Обработка oneof value
        switch (proto.getValueCase()) {
            case BOOL_VALUE:
                builder.setValue(proto.getBoolValue());
                break;
            case INT_VALUE:
                builder.setValue(proto.getIntValue());
                break;
            case VALUE_NOT_SET:
                builder.setValue(null);
                break;
        }

        return builder.build();
    }

    private ConditionTypeAvro mapConditionType(ConditionTypeProto proto) {
        return switch (proto) {
            case MOTION -> ConditionTypeAvro.MOTION;
            case LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case SWITCH -> ConditionTypeAvro.SWITCH;
            case TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case HUMIDITY -> ConditionTypeAvro.HUMIDITY;
            default -> throw new IllegalArgumentException("Unknown condition type: " + proto);
        };
    }

    private ConditionOperationAvro mapConditionOperation(ConditionOperationProto proto) {
        return switch (proto) {
            case EQUALS -> ConditionOperationAvro.EQUALS;
            case GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            case LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
            default -> throw new IllegalArgumentException("Unknown condition operation: " + proto);
        };
    }

    private DeviceActionAvro mapDeviceAction(DeviceActionProto proto) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(proto.getSensorId())
                .setType(mapActionType(proto.getType()));

        if (proto.hasValue()) {
            builder.setValue(proto.getValue());
        }

        return builder.build();
    }

    private ActionTypeAvro mapActionType(ActionTypeProto proto) {
        return switch (proto) {
            case ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case INVERSE -> ActionTypeAvro.INVERSE;
            case SET_VALUE -> ActionTypeAvro.SET_VALUE;
            default -> throw new IllegalArgumentException("Unknown action type: " + proto);
        };
    }

    private Instant convertTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}