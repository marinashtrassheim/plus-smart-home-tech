package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.entity.Action;
import ru.yandex.practicum.model.enums.ActionType;

import java.util.List;
import java.util.Optional;

@Repository
public interface ActionRepository extends JpaRepository<Action, Long> {

    List<Action> findByType(String type);

    List<Action> findByValue(Integer value);

    Optional<Action> findByTypeAndValue(String type, Integer value);
}
