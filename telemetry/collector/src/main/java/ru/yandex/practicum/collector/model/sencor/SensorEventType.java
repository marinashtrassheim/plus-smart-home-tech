package ru.yandex.practicum.collector.model.sencor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum SensorEventType {
    MOTION_SENSOR_EVENT,
    TEMPERATURE_SENSOR_EVENT,
    LIGHT_SENSOR_EVENT,
    CLIMATE_SENSOR_EVENT,
    SWITCH_SENSOR_EVENT;

    @JsonValue
    public String toString() {
        return name();
    }

    @JsonCreator
    public static SensorEventType fromString(String value) {
        return SensorEventType.valueOf(value);
    }
}
