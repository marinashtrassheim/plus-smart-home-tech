package ru.yandex.practicum.collector.model.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum HubEventType {
    DEVICE_ADDED,
    DEVICE_REMOVED,
    SCENARIO_ADDED,
    SCENARIO_REMOVED;

    @JsonValue
    public String toString() {
        return name();
    }

    @JsonCreator
    public static HubEventType fromString(String value) {
        return HubEventType.valueOf(value);
    }
}
