package ru.yandex.practicum.mapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.model.entity.*;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;

import java.util.ArrayList;


@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioEventMapper {

    public Scenario toEntity(ScenarioAddedEventAvro event, String hubId) {
        Scenario scenario = Scenario.builder()
                .hubId(hubId)
                .name(event.getName())
                .scenarioConditions(new ArrayList<>())
                .scenarioActions(new ArrayList<>())
                .build();

        // Маппинг условий
        if (event.getConditions() != null) {
            for (ScenarioConditionAvro cond : event.getConditions()) {
                Condition condition = toConditionEntity(cond);
                ScenarioCondition scenarioCondition = ScenarioCondition.builder()
                        .scenario(scenario)
                        .condition(condition)
                        .sensorId(cond.getSensorId())
                        .build();
                scenario.getScenarioConditions().add(scenarioCondition);
            }
        }

        // Маппинг действий
        if (event.getActions() != null) {
            for (DeviceActionAvro actionAvro : event.getActions()) {
                Action action = toActionEntity(actionAvro);
                ScenarioAction scenarioAction = ScenarioAction.builder()
                        .scenario(scenario)
                        .action(action)
                        .sensorId(actionAvro.getSensorId())
                        .build();
                scenario.getScenarioActions().add(scenarioAction);
            }
        }

        return scenario;
    }

    public Condition toConditionEntity(ScenarioConditionAvro avroCondition) {
        if (avroCondition == null) {
            return null;
        }

        Object valueObj = avroCondition.getValue();
        Integer value = null;

        if (valueObj != null) {
            if (valueObj instanceof Integer) {
                value = (Integer) valueObj;
            } else if (valueObj instanceof Boolean) {
                value = ((Boolean) valueObj) ? 1 : 0;
            } else {
                log.warn("Unexpected value type for condition: {}", valueObj.getClass().getSimpleName());
            }
        }

        Condition condition = Condition.builder()
                .type(avroCondition.getType() != null ? avroCondition.getType().name() : null)
                .operation(avroCondition.getOperation() != null ? avroCondition.getOperation().name() : null)
                .value(value)
                .build();

        log.debug("Created condition: type={}, operation={}, value={}",
                condition.getType(), condition.getOperation(), condition.getValue());

        return condition;
    }

    public Action toActionEntity(DeviceActionAvro avroAction) {
        if (avroAction == null) {
            return null;
        }

        // Для ACTIVATE и DEACTIVATE устанавливаем value=0
        Integer value = avroAction.getValue();
        if (value == null && (avroAction.getType() == ActionTypeAvro.ACTIVATE
                || avroAction.getType() == ActionTypeAvro.DEACTIVATE)) {
            value = 0;
        }

        return Action.builder()
                .type(avroAction.getType() != null ? avroAction.getType().name() : null)
                .value(value)
                .build();
    }
}