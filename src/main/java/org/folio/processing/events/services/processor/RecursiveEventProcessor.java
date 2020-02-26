package org.folio.processing.events.services.processor;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class RecursiveEventProcessor implements EventProcessor {
  private List<EventHandler> eventHandlers = new ArrayList<>();

  @Override
  public CompletableFuture<DataImportEventPayload> process(DataImportEventPayload eventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    String eventType = eventPayload.getEventType();
    Optional<EventHandler> optionalEventHandler = eventHandlers.stream()
      .filter(eventHandler -> eventHandler.getHandlerEventType().equals(eventType))
      .findFirst();
    if (optionalEventHandler.isPresent()) {
      EventHandler nextEventHandler = optionalEventHandler.get();
      processEventRecursively(eventPayload, nextEventHandler).whenComplete((processContext, throwable) -> {
        if (throwable != null) {
          future.completeExceptionally(throwable);
        } else {
          future.complete(eventPayload);
        }
      });
    } else {
      future.complete(eventPayload);
    }
    return future;
  }

  private CompletableFuture<DataImportEventPayload> processEventRecursively(DataImportEventPayload eventPayload, EventHandler eventHandler) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    eventHandler.handle(eventPayload).whenComplete((handlerEventContext, handlerThrowable) -> {
      if (handlerThrowable != null) {
        future.completeExceptionally(handlerThrowable);
      } else {
        String eventType = eventPayload.getEventType();
        Optional<EventHandler> optionalEventHandler = eventHandlers.stream()
          .filter(nextEventHandler -> nextEventHandler.getHandlerEventType().equals(eventType))
          .findFirst();
        if (optionalEventHandler.isPresent()) {
          EventHandler nextEventHandler = optionalEventHandler.get();
          processEventRecursively(eventPayload, nextEventHandler).whenComplete((nextEventContext, nextThrowable) -> {
            if (nextThrowable != null) {
              future.completeExceptionally(handlerThrowable);
            } else {
              future.complete(eventPayload);
            }
          });
        } else {
          future.complete(eventPayload);
        }
      }
    });
    return future;
  }

  @Override
  public List<EventHandler> getEventHandlers() {
    return eventHandlers;
  }
}
