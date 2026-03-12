package ru.yandex.practicum.collector.model.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ConditionOperation {
    EQUALS,
    GREATER_THAN,
    LOWER_THAN;

    @JsonValue
    public String toString() {
        return name();
    }

    @JsonCreator
    public static ConditionOperation fromString(String value) {
        return ConditionOperation.valueOf(value);
    }
}