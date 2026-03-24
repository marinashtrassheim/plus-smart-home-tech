package ru.yandex.practicum.collector.model.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ConditionType {
    MOTION,
    LUMINOSITY,
    SWITCH,
    TEMPERATURE,
    CO2LEVEL,
    HUMIDITY;

    @JsonValue
    public String toString() {
        return name();
    }

    @JsonCreator
    public static ConditionType fromString(String value) {
        return ConditionType.valueOf(value);
    }
}
