package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.entity.Sensor;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Repository
public interface SensorRepository extends JpaRepository<Sensor, String> {

    /**
     * Найти все датчики, принадлежащие хабу
     */
    List<Sensor> findByHubId(String hubId);

    /**
     * Проверить существование датчика с указанным id и hubId
     */
    boolean existsByIdAndHubId(String id, String hubId);

    /**
     * Найти датчик по id и hubId
     */
    Optional<Sensor> findByIdAndHubId(String id, String hubId);

    /**
     * Проверить, что все датчики из списка принадлежат указанному хабу
     */
    boolean existsByIdInAndHubId(Collection<String> ids, String hubId);

    /**
     * Удалить все датчики хаба
     */
    void deleteByHubId(String hubId);
}