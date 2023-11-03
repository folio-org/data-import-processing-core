package org.folio.processing.events.services.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventHandlerNotFoundException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.processing.events.EventManager.OL_ACCUMULATIVE_RESULTS;
import static org.folio.processing.events.EventManager.POST_PROCESSING_INDICATOR;
import static org.folio.processing.events.EventManager.POST_PROCESSING_RESULT_EVENT_KEY;

public class EventProcessorImpl implements EventProcessor {

  private static final Logger LOG = LogManager.getLogger(EventProcessorImpl.class);

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
        eventHandler.handle(eventPayload)
          .thenApply(dataImportEventPayload -> eventHandler.isPostProcessingNeeded() ? preparePayloadForPostProcessing(dataImportEventPayload, eventHandler) : dataImportEventPayload)
          .whenComplete((payload, throwable) -> {
            logEventProcessingTime(eventType, startTime, eventPayload);
            updatePayloadIfNeeded(payload);
            if (throwable != null) {
              future.completeExceptionally(throwable);
            } else {
              future.complete(payload);
            }
          });
      } else {
        LOG.info("process:: No suitable handler found for {} event type and current profile {}", eventPayload.getEventType(), eventPayload.getCurrentNode().getContentType());
        future.completeExceptionally(new EventHandlerNotFoundException(format("No suitable handler found for %s event type", eventPayload.getEventType())));
      }
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  private DataImportEventPayload preparePayloadForPostProcessing(DataImportEventPayload dataImportEventPayload, EventHandler eventHandler) {
    dataImportEventPayload.getContext().put(POST_PROCESSING_INDICATOR, Boolean.toString(true));
    dataImportEventPayload.getContext().put(POST_PROCESSING_RESULT_EVENT_KEY, dataImportEventPayload.getEventType());
    dataImportEventPayload.setEventType(eventHandler.getPostProcessingInitializationEventType());
    return dataImportEventPayload;
  }

  @Override
  public List<EventHandler> getEventHandlers() {
    return eventHandlers;
  }

  private void logEventProcessingTime(String eventType, long startTime, DataImportEventPayload eventPayload) {
    try {
      var endTime = System.nanoTime();
      final String lastEvent = getLastEvent(eventPayload);
      if (DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value().equals(lastEvent)) {
        LOG.debug("logEventProcessingTime:: Event '{}' has been processed for {} ms", lastEvent, (endTime - startTime) / 1000000L);
      } else {
        String profileType = eventPayload.getCurrentNode().getContentType().toString();
        String profileId = eventPayload.getCurrentNode().getProfileId();
        LOG.debug("logEventProcessingTime:: Event '{}' has been processed using {} with id '{}' for {} ms", eventType, profileType, profileId, (endTime - startTime) / 1000000L);
      }
    } catch (Exception e) {
      LOG.warn("logEventProcessingTime:: An Exception occurred {}", e.getMessage());
    }
  }

  private String getLastEvent(DataImportEventPayload eventPayload) {
    final var eventsChain = eventPayload.getEventsChain();
    return eventsChain.get(eventsChain.size() - 1);
  }

  private void updatePayloadIfNeeded(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getContext().remove(OL_ACCUMULATIVE_RESULTS);
  }
}
