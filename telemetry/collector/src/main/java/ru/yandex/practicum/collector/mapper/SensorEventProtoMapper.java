package ru.yandex.practicum.collector.mapper;

import com.google.protobuf.Timestamp;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

@Component
public class SensorEventProtoMapper {

    public SensorEventAvro mapToAvro(SensorEventProto proto) {
        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(proto.getId())
                .setHubId(proto.getHubId())
                .setTimestamp(convertTimestamp(proto.getTimestamp()));

        switch (proto.getPayloadCase()) {
            case MOTION_SENSOR:
                MotionSensorProto motionProto = proto.getMotionSensor();
                builder.setPayload(MotionSensorAvro.newBuilder()
                        .setLinkQuality(motionProto.getLinkQuality())
                        .setMotion(motionProto.getMotion())
                        .setVoltage(motionProto.getVoltage())
                        .build());
                break;

            case TEMPERATURE_SENSOR:
                TemperatureSensorProto tempProto = proto.getTemperatureSensor();
                builder.setPayload(TemperatureSensorAvro.newBuilder()
                        .setTemperatureC(tempProto.getTemperatureC())
                        .setTemperatureF(tempProto.getTemperatureF())
                        .build());
                break;

            case LIGHT_SENSOR:
                LightSensorProto lightProto = proto.getLightSensor();
                builder.setPayload(LightSensorAvro.newBuilder()
                        .setLinkQuality(lightProto.getLinkQuality())
                        .setLuminosity(lightProto.getLuminosity())
                        .build());
                break;

            case CLIMATE_SENSOR:
                ClimateSensorProto climateProto = proto.getClimateSensor();
                builder.setPayload(ClimateSensorAvro.newBuilder()
                        .setTemperatureC(climateProto.getTemperatureC())
                        .setHumidity(climateProto.getHumidity())
                        .setCo2Level(climateProto.getCo2Level())
                        .build());
                break;

            case SWITCH_SENSOR:
                SwitchSensorProto switchProto = proto.getSwitchSensor();
                builder.setPayload(SwitchSensorAvro.newBuilder()
                        .setState(switchProto.getState())
                        .build());
                break;

            default:
                throw new IllegalArgumentException("Unknown sensor event type: " + proto.getPayloadCase());
        }
        return builder.build();
    }

    private Instant convertTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}