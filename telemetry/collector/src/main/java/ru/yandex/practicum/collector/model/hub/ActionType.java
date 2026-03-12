package ru.yandex.practicum.collector.model.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ActionType {
    ACTIVATE,
    DEACTIVATE,
    INVERSE,
    SET_VALUE;

    @JsonValue
    public String toString() {
        return name();
    }

    @JsonCreator
    public static ActionType fromString(String value) {
        return ActionType.valueOf(value);
    }
}
