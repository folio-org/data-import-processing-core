package org.folio.processing.events;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.events.services.processor.EventProcessor;
import org.folio.processing.events.services.processor.EventProcessorImpl;
import org.folio.processing.events.services.publisher.EventPublisher;
import org.folio.processing.events.services.publisher.RestEventPublisher;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.DataImportEventTypes.DI_ERROR;

/**
 * The central class to use for handlers registration and event handling.
 */
public final class EventManager {

  private static final EventProcessor eventProcessor = new EventProcessorImpl();
  private static final EventPublisher eventPublisher = new RestEventPublisher();

  private EventManager() {
  }

  /**
   * Handles the given payload of event.
   * If there are handlers found to handle event then the EventManager calls EventProcessor passing event payload.
   * After processing the EventManager calls EventPublisher to send next event up to the queue.
   *
   * @param eventPayload event payload
   * @return future with event payload after handling
   */
  public static CompletableFuture<DataImportEventPayload> handleEvent(DataImportEventPayload eventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    eventProcessor.process(eventPayload)
      .whenComplete((processPayload, processThrowable) ->
        eventPublisher.publish(prepareEventPayload(eventPayload, processThrowable))
        .whenComplete((publishPayload, publishThrowable) -> {
        if (publishThrowable == null) {
          future.complete(eventPayload);
        } else {
          future.completeExceptionally(publishThrowable);
        }
      }));
    return future;
  }

  /**
   * Prepares given eventPayload for publishing.
   *
   * @param eventPayload eventPayload
   */
  private static DataImportEventPayload prepareEventPayload(DataImportEventPayload eventPayload, Throwable throwable) {
    // update currentNode
    // update currentNodePath
    if (throwable != null) {
      return prepareErrorEventPayload(eventPayload, throwable);
    }
    List<ProfileSnapshotWrapper> children = eventPayload.getCurrentNode().getChildSnapshotWrappers();
    if (isNotEmpty(children)) {
      eventPayload.getCurrentNodePath().add(eventPayload.getCurrentNode().getId());
      eventPayload.setCurrentNode(children.get(0));
    } else {
      // TODO search in jobProfile tree, if finished - fire DI_COMPLETED event
      eventPayload.getEventsChain().add(eventPayload.getEventType());
      eventPayload.setEventType(DI_COMPLETED.value());
    }
    return eventPayload;
  }

  private static DataImportEventPayload prepareErrorEventPayload(DataImportEventPayload eventPayload, Throwable throwable) {
    eventPayload.setEventType(DI_ERROR.value());
    // an error occurred during handling of current event type, so it is pushed to the events chain
    eventPayload.getEventsChain().add(eventPayload.getEventType());
    eventPayload.getContext().put("ERROR", throwable.getMessage());
    return eventPayload;
  }

  /**
   * Performs registration for given event handler in processing list
   *
   * @param eventHandler event handler
   * @return true handlers is registered
   */
  public static <T extends EventHandler> boolean registerEventHandler(T eventHandler) {
    return eventProcessor.getEventHandlers().add(eventHandler);
  }

  /**
   * Clears the registry of event handlers.
   */
  public static void clearEventHandlers() {
    eventProcessor.getEventHandlers().clear();
  }
}
