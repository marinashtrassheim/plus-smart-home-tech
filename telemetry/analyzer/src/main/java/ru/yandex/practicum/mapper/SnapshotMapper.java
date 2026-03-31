package ru.yandex.practicum.mapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.model.dto.ActionDto;
import ru.yandex.practicum.model.dto.ConditionDto;
import ru.yandex.practicum.model.dto.ScenarioDto;
import ru.yandex.practicum.model.entity.Action;
import ru.yandex.practicum.model.entity.Condition;
import ru.yandex.practicum.model.entity.Scenario;
import ru.yandex.practicum.model.enums.ActionType;
import ru.yandex.practicum.model.enums.ConditionOperation;
import ru.yandex.practicum.model.enums.ConditionType;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotMapper {

    public ScenarioDto toDto(Scenario scenario) {
        if (scenario == null) {
            return null;
        }
        List<ConditionDto> conditionDtos = scenario.getScenarioConditions().stream()
                .map(sc -> toConditionDto(sc.getCondition(), sc.getSensorId()))
                .collect(Collectors.toList());

        List<ActionDto> actionDtos = scenario.getScenarioActions().stream()
                .map(sa -> toActionDto(sa.getAction(), sa.getSensorId()))
                .collect(Collectors.toList());

        return ScenarioDto.builder()
                .id(scenario.getId())
                .hubId(scenario.getHubId())
                .name(scenario.getName())
                .conditions(conditionDtos)
                .actions(actionDtos)
                .build();
    }

    public ConditionDto toConditionDto(Condition condition, String sensorId) {
        if (condition == null) {
            return null;
        }

        return ConditionDto.builder()
                .id(condition.getId())
                .sensorId(sensorId)
                .type(parseConditionType(condition.getType()))
                .operation(parseConditionOperation(condition.getOperation()))
                .value(condition.getValue())
                .build();
    }

    public ActionDto toActionDto(Action action, String sensorId) {
        if (action == null) {
            return null;
        }

        return ActionDto.builder()
                .id(action.getId())
                .sensorId(sensorId)
                .type(parseActionType(action.getType()))
                .value(action.getValue())
                .build();
    }

    // ========== Преобразование String → enum ==========

    public ConditionType parseConditionType(String type) {
        if (type == null) return null;
        try {
            return ConditionType.valueOf(type);
        } catch (IllegalArgumentException e) {
            log.warn("Unknown condition type: {}", type);
            return null;
        }
    }

    public ConditionOperation parseConditionOperation(String operation) {
        if (operation == null) return null;
        try {
            return ConditionOperation.valueOf(operation);
        } catch (IllegalArgumentException e) {
            log.warn("Unknown condition operation: {}", operation);
            return null;
        }
    }

    private ActionType parseActionType(String type) {
        if (type == null) return null;
        try {
            return ActionType.valueOf(type);
        } catch (IllegalArgumentException e) {
            log.warn("Unknown action type: {}", type);
            return null;
        }
    }
}
