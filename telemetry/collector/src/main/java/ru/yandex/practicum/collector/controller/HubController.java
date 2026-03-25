package ru.yandex.practicum.collector.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.collector.handler.hub.HubEventHandler;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@GrpcService
public class HubController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final Map<HubEventProto.PayloadCase, HubEventHandler> handlers;

    public HubController(Set<HubEventHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toMap(
                        HubEventHandler::getMessageType,
                        Function.identity()
                ));
    }

    @Override
    public void sendHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Received gRPC hub event: hubId={}, type={}",
                    request.getHubId(),
                    request.getPayloadCase());

            // Находим нужный обработчик по типу события
            HubEventHandler handler = handlers.get(request.getPayloadCase());

            if (handler == null) {
                throw new IllegalArgumentException(
                        "No handler found for event type: " + request.getPayloadCase()
                );
            }

            // Обрабатываем событие
            handler.handle(request);

            // Отправляем успешный ответ
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error processing sensor event: {}", e.getMessage(), e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }
}
