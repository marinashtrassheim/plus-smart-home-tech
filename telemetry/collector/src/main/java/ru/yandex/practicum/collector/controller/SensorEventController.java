package ru.yandex.practicum.collector.controller;


import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.collector.model.sencor.SensorEvent;
import ru.yandex.practicum.collector.service.SensorEventService;


@Slf4j
@RestController
@RequestMapping("/events/sensors")
@RequiredArgsConstructor
public class SensorEventController {
    private final SensorEventService sensorEventService;

    @Value("${kafka.topic.sensors}")
    private String sensorsTopic;

    @PostMapping
    public void collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("Received sensor event: {}", event);
        sensorEventService.processEvent(event, sensorsTopic);
    }
}
