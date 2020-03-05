package org.folio.processing.events.services.processor;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;

public class EventProcessorImpl implements EventProcessor {

  private List<EventHandler> eventHandlers = new ArrayList<>();

  @Override
  public CompletableFuture<DataImportEventPayload> process(DataImportEventPayload eventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      Optional<EventHandler> optionalEventHandler = eventHandlers.stream()
        .filter(eventHandler -> eventHandler.isEligible(eventPayload))
        .findFirst();
      if (optionalEventHandler.isPresent()) {
        EventHandler eventHandler = optionalEventHandler.get();
        eventHandler.handle(eventPayload).whenComplete((payload, throwable) -> {
          if (throwable != null) {
            future.completeExceptionally(throwable);
          } else {
            future.complete(payload);
          }
        });
      } else {
        future.completeExceptionally(new EventProcessingException(format("No suitable handler found for %s event type", eventPayload.getEventType())));
      }
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public List<EventHandler> getEventHandlers() {
    return eventHandlers;
  }
}
