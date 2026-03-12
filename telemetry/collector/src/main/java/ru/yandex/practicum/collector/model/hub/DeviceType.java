package ru.yandex.practicum.collector.model.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum DeviceType {
    MOTION_SENSOR,
    TEMPERATURE_SENSOR,
    LIGHT_SENSOR,
    CLIMATE_SENSOR,
    SWITCH_SENSOR;

    @JsonValue
    public String toString() {
        return name();
    }

    @JsonCreator
    public static DeviceType fromString(String value) {
        return DeviceType.valueOf(value);
    }
}