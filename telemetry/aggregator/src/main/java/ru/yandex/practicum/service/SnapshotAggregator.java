package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.repository.SnapshotRepository;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Агрегатор событий датчиков в снапшоты.
 * Реализует логику обновления состояния снапшота на основе входящего события.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotAggregator {

    private final SnapshotRepository repository;

    /**
     * Обновляет состояние снапшота на основе события датчика.
     * Реализует алгоритм из псевдокода задания:
     * 1. Проверяем, есть ли снапшот для hubId
     * 2. Если нет - создаем новый
     * 3. Если есть - проверяем, нужно ли обновлять данные датчика
     * 4. Если нужно - обновляем и возвращаем обновленный снапшот
     * 5. Если не нужно - возвращаем empty
     *
     * @param event событие датчика
     * @return Optional с обновленным снапшотом, если состояние изменилось
     */
    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();
        Instant eventTimestamp = event.getTimestamp();
        log.info("=== updateState called: hubId={}, sensorId={}, timestamp={} ===",
                hubId, sensorId, eventTimestamp);

        // 1. Проверяем, есть ли снапшот для хаба
        Optional<SensorsSnapshotAvro> existingSnapshot = repository.findByHubId(hubId);

        SensorsSnapshotAvro snapshot;
        Map<String, SensorStateAvro> sensorsState;

        if (existingSnapshot.isPresent()) {
            // Снапшот существует
            snapshot = existingSnapshot.get();
            sensorsState = new HashMap<>(snapshot.getSensorsState());

            // 2. Проверяем, есть ли данные для этого датчика
            SensorStateAvro oldState = sensorsState.get(sensorId);

            if (oldState != null) {
                // Данные датчика уже есть в снапшоте
                Instant oldTimestamp = oldState.getTimestamp();

                // 3. Проверяем, нужно ли обновлять
                if (oldTimestamp.isAfter(eventTimestamp)) {
                    // Пришедшее событие старше, чем данные в снапшоте - игнорируем
                    log.info("Ignoring old event: eventTimestamp={} < oldTimestamp={}",
                            eventTimestamp, oldTimestamp);
                    return Optional.empty();
                }

                // Проверяем, изменились ли данные
                if (oldTimestamp.equals(eventTimestamp) && dataEquals(oldState.getData(), event.getPayload())) {
                    // Данные не изменились - игнорируем
                    log.info("Ignoring duplicate event: timestamp and data are the same");
                    return Optional.empty();
                }
            }

            // 4. Обновляем таймстемп снапшота
            if (eventTimestamp.isAfter(snapshot.getTimestamp())) {
                snapshot.setTimestamp(eventTimestamp);
            }

        } else {
            // Снапшота нет - создаем новый
            log.info("Creating new snapshot for hub: {}", hubId);
            snapshot = SensorsSnapshotAvro.newBuilder()
                    .setHubId(hubId)
                    .setTimestamp(eventTimestamp)
                    .setSensorsState(new HashMap<>())
                    .build();
            sensorsState = new HashMap<>();
        }

        // 5. Создаем новое состояние датчика
        SensorStateAvro newSensorState = SensorStateAvro.newBuilder()
                .setTimestamp(eventTimestamp)
                .setData(event.getPayload())
                .build();

        // 6. Обновляем состояние датчика в снапшоте
        sensorsState.put(sensorId, newSensorState);
        snapshot.setSensorsState(sensorsState);

        // 7. Сохраняем обновленный снапшот в репозиторий
        repository.save(snapshot);

        log.info("Snapshot updated for hub: {}, total sensors: {}",
                hubId, sensorsState.size());

        return Optional.of(snapshot);
    }

    /**
     * Сравнивает данные двух датчиков на равенство.
     * Так как data имеет тип union, используем прямое сравнение объектов.
     *
     * @param data1 данные первого датчика
     * @param data2 данные второго датчика
     * @return true если данные равны
     */
    private boolean dataEquals(Object data1, Object data2) {
        if (data1 == null && data2 == null) {
            return true;
        }
        if (data1 == null || data2 == null) {
            return false;
        }
        return data1.equals(data2);
    }
}