package ru.yandex.practicum.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioDto {
    private Long id;
    private String hubId;
    private String name;

    @Builder.Default
    private List<ConditionDto> conditions = new ArrayList<>();

    @Builder.Default
    private List<ActionDto> actions = new ArrayList<>();
}

