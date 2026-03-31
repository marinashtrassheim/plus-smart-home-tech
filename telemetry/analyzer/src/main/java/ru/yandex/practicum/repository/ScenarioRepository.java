package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.entity.Scenario;

import java.util.List;
import java.util.Optional;

public interface ScenarioRepository extends JpaRepository<Scenario, Long> {
    /**
     * Найти все сценарии для хаба
     */
    List<Scenario> findByHubId(String hubId);

    /**
     * Найти сценарий по хабу и имени
     */
    Optional<Scenario> findByHubIdAndName(String hubId, String name);

    /**
     * Проверить существование сценария
     */
    boolean existsByHubIdAndName(String hubId, String name);

    /**
     * Удалить сценарий по хабу и имени
     */
    void deleteByHubIdAndName(String hubId, String name);

    /**
     * Удалить все сценарии хаба
     */
    void deleteByHubId(String hubId);
}
