package ru.yandex.practicum.service;


import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.dto.ActionDto;
import ru.yandex.practicum.model.dto.ScenarioDto;
import ru.yandex.practicum.model.enums.ActionType;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

import java.time.Instant;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceActionSender {

    @GrpcClient("hub-router")
    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public void sendActions(String hubId, List<ScenarioDto> triggeredScenarios, Instant timestamp) {
        for (ScenarioDto scenario : triggeredScenarios) {
            for (ActionDto action : scenario.getActions()) {
                sendAction(hubId, scenario.getName(), action, timestamp);
            }
        }
    }

    private void sendAction(String hubId, String scenarioName, ActionDto action, Instant timestamp) {
        try {
            DeviceActionProto deviceAction = DeviceActionProto.newBuilder()
                    .setSensorId(action.getSensorId())
                    .setType(mapActionType(action.getType()))
                    .setValue(action.getValue())
                    .build();

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(hubId)
                    .setScenarioName(scenarioName)
                    .setAction(deviceAction)
                    .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                            .setSeconds(timestamp.getEpochSecond())
                            .setNanos(timestamp.getNano()))
                    .build();

            hubRouterClient.handleDeviceAction(request);
            log.debug("Action sent: hubId={}, scenario={}, actionType={}, value={}",
                    hubId, scenarioName, action.getType(), action.getValue());

        } catch (StatusRuntimeException e) {
            log.error("gRPC error sending action: hubId={}, scenario={}, status={}",
                    hubId, scenarioName, e.getStatus(), e);
        } catch (Exception e) {
            log.error("Failed to send action: hubId={}, scenario={}, actionType={}",
                    hubId, scenarioName, action.getType(), e);
        }
    }

    /**
     * Маппинг ActionType (нашего enum) → ActionTypeProto (gRPC enum)
     */
    private ActionTypeProto mapActionType(ActionType type) {
        return switch (type) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
        };
    }
}