package ru.yandex.practicum.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.model.enums.DeviceType;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorDto {
    private String id;
    private String hubId;
    private DeviceType deviceType;
}
