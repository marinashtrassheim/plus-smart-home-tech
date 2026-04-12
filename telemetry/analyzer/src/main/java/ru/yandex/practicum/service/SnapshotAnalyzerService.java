package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.mapper.SnapshotMapper;
import ru.yandex.practicum.model.dto.ScenarioDto;
import ru.yandex.practicum.model.entity.Condition;
import ru.yandex.practicum.model.entity.Scenario;
import ru.yandex.practicum.model.entity.ScenarioCondition;
import ru.yandex.practicum.model.enums.ConditionOperation;
import ru.yandex.practicum.model.enums.ConditionType;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotAnalyzerService {

    private final ScenarioRepository scenarioRepository;
    private final DeviceActionSender deviceActionSender;
    private final SnapshotMapper snapshotMapper;

    @Transactional(readOnly = true)
    public void analyzeSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        Instant snapshotTimestamp = snapshot.getTimestamp();

        log.debug("Analyzing snapshot for hub: {}, timestamp: {}", hubId, snapshotTimestamp);

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        if (scenarios.isEmpty()) {
            log.debug("No scenarios found for hub: {}", hubId);
            return;
        }

        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();
        List<ScenarioDto> triggeredScenarios = new ArrayList<>();
        log.info("Found {} scenarios for hub: {}", scenarios.size(), hubId);

        for (Scenario scenario : scenarios) {
            if (checkScenarioConditions(scenario, sensorsState)) {
                triggeredScenarios.add(snapshotMapper.toDto(scenario));
                log.info("Scenario triggered: hubId={}, scenarioName={}", hubId, scenario.getName());
            }
        }

        if (!triggeredScenarios.isEmpty()) {
            deviceActionSender.sendActions(hubId, triggeredScenarios, snapshotTimestamp);
        } else {
            log.debug("No scenarios triggered for hub: {}", hubId);
        }
    }

    private boolean checkScenarioConditions(Scenario scenario, Map<String, SensorStateAvro> sensorsState) {
        List<ScenarioCondition> scenarioConditions = scenario.getScenarioConditions();

        if (scenarioConditions == null || scenarioConditions.isEmpty()) {
            log.debug("Scenario has no conditions: {}", scenario.getName());
            return true;
        }

        return scenarioConditions.stream()
                .allMatch(scenarioCondition -> evaluateCondition(scenarioCondition, sensorsState));
    }

    private boolean evaluateCondition(ScenarioCondition scenarioCondition, Map<String, SensorStateAvro> sensorsState) {
        Condition condition = scenarioCondition.getCondition();
        String sensorId = scenarioCondition.getSensorId();

        ConditionType type = snapshotMapper.parseConditionType(condition.getType());
        ConditionOperation operation = snapshotMapper.parseConditionOperation(condition.getOperation());
        Integer expectedValue = condition.getValue();

        if (type == null || operation == null) {
            log.debug("Invalid condition type or operation: type={}, operation={}",
                    condition.getType(), condition.getOperation());
            return false;
        }

        SensorStateAvro sensorState = sensorsState.get(sensorId);
        if (sensorState == null) {
            log.debug("No sensor found for id: {}", sensorId);
            return false;
        }

        if (!canProvideValue(sensorState, type)) {
            log.debug("Sensor {} cannot provide value for type: {}", sensorId, type);
            return false;
        }

        Integer actualValue = extractValue(sensorState, type);
        if (actualValue == null) {
            log.debug("Could not extract value from sensor {} for type: {}", sensorId, type);
            return false;
        }

        return compareValues(actualValue, expectedValue, operation);
    }

    private boolean canProvideValue(SensorStateAvro sensorState, ConditionType type) {
        Object data = sensorState.getData();
        return switch (type) {
            case TEMPERATURE -> data instanceof TemperatureSensorAvro || data instanceof ClimateSensorAvro;
            case HUMIDITY -> data instanceof ClimateSensorAvro;
            case CO2LEVEL -> data instanceof ClimateSensorAvro;
            case MOTION -> data instanceof MotionSensorAvro;
            case LUMINOSITY -> data instanceof LightSensorAvro;
            case SWITCH -> data instanceof SwitchSensorAvro;
        };
    }

    private Integer extractValue(SensorStateAvro sensorState, ConditionType type) {
        Object data = sensorState.getData();
        return switch (type) {
            case TEMPERATURE -> {
                if (data instanceof TemperatureSensorAvro temp) {
                    yield temp.getTemperatureC();
                } else if (data instanceof ClimateSensorAvro climate) {
                    yield climate.getTemperatureC();
                }
                yield null;
            }
            case HUMIDITY -> {
                if (data instanceof ClimateSensorAvro climate) {
                    yield climate.getHumidity();
                }
                yield null;
            }
            case CO2LEVEL -> {
                if (data instanceof ClimateSensorAvro climate) {
                    yield climate.getCo2Level();
                }
                yield null;
            }
            case MOTION -> {
                if (data instanceof MotionSensorAvro motion) {
                    yield motion.getMotion() ? 1 : 0;
                }
                yield null;
            }
            case LUMINOSITY -> {
                if (data instanceof LightSensorAvro light) {
                    yield light.getLuminosity();
                }
                yield null;
            }
            case SWITCH -> {
                if (data instanceof SwitchSensorAvro switchSensor) {
                    yield switchSensor.getState() ? 1 : 0;
                }
                yield null;
            }
        };
    }

    private boolean compareValues(Integer actual, Integer expected, ConditionOperation operation) {
        if (actual == null || expected == null) {
            return false;
        }
        return switch (operation) {
            case EQUALS -> actual.equals(expected);
            case GREATER_THAN -> actual > expected;
            case LOWER_THAN -> actual < expected;
        };
    }
}