package ru.yandex.practicum.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.model.enums.ActionType;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ActionDto {
    private Long id;
    private String sensorId;
    private ActionType type;
    private Integer value;
}