package org.folio.processing.events.services.processor;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;

public class EventProcessorImpl implements EventProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(EventProcessorImpl.class);

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
        String eventType = eventPayload.getEventType();
        long startTime = System.nanoTime();
        eventHandler.handle(eventPayload).whenComplete((payload, throwable) -> {
          logEventProcessingTime(eventType, startTime, eventPayload);
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

  private void logEventProcessingTime(String eventType, long startTime, DataImportEventPayload eventPayload) {
    long endTime = System.nanoTime();
    String profileType = eventPayload.getCurrentNode().getContentType().toString();
    String profileId = eventPayload.getCurrentNode().getProfileId();
    LOG.debug("Event '" + eventType + "' has been processed using " + profileType + " with id '" + profileId + "' for " + (endTime - startTime) / 1000000L + " ms");
  }
}
