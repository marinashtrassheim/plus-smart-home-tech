package ru.yandex.practicum.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Хранилище снапшотов в памяти.
 * Хранит актуальные снапшоты для каждого хаба.
 * Потокобезопасно благодаря ConcurrentHashMap.
 */
@Slf4j
@Component
public class SnapshotRepository {

    // Ключ - hubId, значение - текущий снапшот хаба
    private final Map<String, SensorsSnapshotAvro> snapshots = new ConcurrentHashMap<>();

    /**
     * Возвращает снапшот для указанного хаба, если он существует.
     *
     * @param hubId идентификатор хаба
     * @return Optional с снапшотом или empty, если снапшота нет
     */
    public Optional<SensorsSnapshotAvro> findByHubId(String hubId) {
        return Optional.ofNullable(snapshots.get(hubId));
    }

    /**
     * Сохраняет или обновляет снапшот для хаба.
     *
     * @param snapshot снапшот для сохранения
     * @return предыдущий снапшот или null, если его не было
     */
    public SensorsSnapshotAvro save(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        log.debug("Saving snapshot for hub: {}", hubId);
        return snapshots.put(hubId, snapshot);
    }

    /**
     * Проверяет, существует ли снапшот для указанного хаба.
     *
     * @param hubId идентификатор хаба
     * @return true если снапшот существует
     */
    public boolean exists(String hubId) {
        return snapshots.containsKey(hubId);
    }

    /**
     * Возвращает все снапшоты (для отладки).
     *
     * @return копия карты со всеми снапшотами
     */
    public Map<String, SensorsSnapshotAvro> findAll() {
        return new ConcurrentHashMap<>(snapshots);
    }

    /**
     * Удаляет снапшот для указанного хаба.
     *
     * @param hubId идентификатор хаба
     */
    public void delete(String hubId) {
        log.debug("Deleting snapshot for hub: {}", hubId);
        snapshots.remove(hubId);
    }

    /**
     * Очищает все снапшоты.
     */
    public void clear() {
        log.debug("Clearing all snapshots");
        snapshots.clear();
    }
}