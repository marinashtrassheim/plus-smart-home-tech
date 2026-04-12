package ru.yandex.practicum.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.mapper.HubEventMapper;
import ru.yandex.practicum.model.entity.*;
import ru.yandex.practicum.model.enums.ActionType;
import ru.yandex.practicum.repository.*;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventService {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final HubEventMapper hubEventMapper;

    @Transactional
    public void processHubEvent(HubEventAvro event) {
        if (event == null) {
            log.warn("Received null hub event");
            return;
        }

        String hubId = event.getHubId();
        log.debug("Processing hub event: hubId={}, timestamp={}", hubId, event.getTimestamp());

        Optional<HubEventMapper.ProcessedEvent> processed = hubEventMapper.process(event);

        if (processed.isEmpty()) {
            log.warn("Could not process hub event: unknown payload type");
            return;
        }

        HubEventMapper.ProcessedEvent processedEvent = processed.get();

        switch (processedEvent.getType()) {
            case DEVICE_ADDED:
                handleDeviceAdded((Sensor) processedEvent.getData());
                break;
            case DEVICE_REMOVED:
                handleDeviceRemoved((String) processedEvent.getData(), processedEvent.getHubId());
                break;
            case SCENARIO_ADDED:
                handleScenarioAdded((Scenario) processedEvent.getData());
                break;
            case SCENARIO_REMOVED:
                handleScenarioRemoved((String) processedEvent.getData(), processedEvent.getHubId());
                break;
        }
    }

    private void handleDeviceAdded(Sensor sensor) {
        if (sensorRepository.existsById(sensor.getId())) {
            log.debug("Sensor already exists: {}", sensor.getId());
            return;
        }
        sensorRepository.save(sensor);
        log.info("Device added: id={}, hubId={}, type={}",
                sensor.getId(), sensor.getHubId(), sensor.getDeviceType());
    }

    private void handleDeviceRemoved(String sensorId, String hubId) {
        Optional<Sensor> sensor = sensorRepository.findByIdAndHubId(sensorId, hubId);
        if (sensor.isEmpty()) {
            log.debug("Sensor not found for removal: id={}, hubId={}", sensorId, hubId);
            return;
        }
        sensorRepository.delete(sensor.get());
        log.info("Device removed: id={}, hubId={}", sensorId, hubId);
    }

    private void handleScenarioAdded(Scenario scenario) {
        Optional<Scenario> existing = scenarioRepository.findByHubIdAndName(
                scenario.getHubId(), scenario.getName());

        if (existing.isPresent()) {
            log.info("Scenario already exists: hubId={}, name={}",
                    scenario.getHubId(), scenario.getName());
            Scenario existingScenario = existing.get();

            existingScenario.getScenarioConditions().clear();
            existingScenario.getScenarioConditions().addAll(scenario.getScenarioConditions());
            existingScenario.getScenarioActions().clear();
            existingScenario.getScenarioActions().addAll(scenario.getScenarioActions());

            existingScenario.getScenarioConditions().forEach(sc -> sc.setScenario(existingScenario));
            existingScenario.getScenarioActions().forEach(sa -> sa.setScenario(existingScenario));

            scenarioRepository.save(existingScenario);
            log.info("Scenario updated: hubId={}, name={}",
                    scenario.getHubId(), scenario.getName());
        } else {

            // Сохраняем все Condition
            for (ScenarioCondition sc : scenario.getScenarioConditions()) {
                Condition condition = sc.getCondition();
                Condition existingCondition = conditionRepository.findByTypeAndOperationAndValue(
                        condition.getType(), condition.getOperation(), condition.getValue()).stream().findFirst().orElse(null);
                if (existingCondition != null) {
                    sc.setCondition(existingCondition);
                } else {
                    conditionRepository.save(condition);
                }
            }

            // Сохраняем все Action
            for (ScenarioAction sa : scenario.getScenarioActions()) {
                Action action = sa.getAction();
                Action existingAction = actionRepository.findByTypeAndValue(
                        action.getType(), action.getValue()).stream().findFirst().orElse(null);
                if (existingAction != null) {
                    sa.setAction(existingAction);
                } else {
                    actionRepository.save(action);
                }
            }

            // Сохраняем Scenario
            scenarioRepository.save(scenario);
            log.info("Scenario added: hubId={}, name={}",
                    scenario.getHubId(), scenario.getName());
        }
    }

    private void handleScenarioRemoved(String scenarioName, String hubId) {
        Optional<Scenario> scenario = scenarioRepository.findByHubIdAndName(hubId, scenarioName);
        if (scenario.isEmpty()) {
            log.debug("Scenario not found for removal: hubId={}, name={}", hubId, scenarioName);
            return;
        }
        scenarioRepository.delete(scenario.get());
        log.info("Scenario removed: hubId={}, name={}", hubId, scenarioName);
    }
}