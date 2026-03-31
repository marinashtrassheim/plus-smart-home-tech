package ru.yandex.practicum.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.model.enums.ConditionType;
import ru.yandex.practicum.model.enums.ConditionOperation;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConditionDto {
    private Long id;
    private String sensorId;
    private ConditionType type;
    private ConditionOperation operation;
    private Integer value;
}
