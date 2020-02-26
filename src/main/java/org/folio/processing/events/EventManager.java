package org.folio.processing.events;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.AbstractEventHandler;
import org.folio.processing.events.services.processor.EventProcessor;
import org.folio.processing.events.services.processor.RecursiveEventProcessor;
import org.folio.processing.events.services.publisher.EventPublisher;
import org.folio.processing.events.services.publisher.RestEventPublisher;

import java.util.concurrent.CompletableFuture;

/**
 * The central class to use for handlers registration and event handling.
 */
public final class EventManager {
  private static final EventProcessor eventProcessor = new RecursiveEventProcessor();
  private static final EventPublisher eventPublisher = new RestEventPublisher();

  private EventManager() {
  }

  /**
   * Handles the given payload of event.
   * If there are handlers found to handle event then the EventManager calls EventProcessor passing event context.
   * After processing the EventManager calls EventPublisher to send next event up to the queue.
   *
   * @param eventPayload event payload as a string
   * @return future with event context
   */
  public static CompletableFuture<DataImportEventPayload> handleEvent(DataImportEventPayload eventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    eventProcessor.process(eventPayload).whenComplete((processContext, processThrowable) -> {
      if (processThrowable != null) {
        future.completeExceptionally(processThrowable);
      } else {
          prepareContext(eventPayload);
          eventPublisher.publish(eventPayload).whenComplete((publishContext, publishThrowable) -> {
            if (publishThrowable != null) {
              future.completeExceptionally(publishThrowable);
            } else {
              future.complete(eventPayload);
            }
          });
      }
    });
    return future;
  }

  /**
   * Prepares given eventPayload for publishing.
   *
   * @param eventPayload eventPayload
   */
  private static void prepareContext(DataImportEventPayload eventPayload) {
    // update currentNode
    // update currentNodePath
  }

  /**
   * Performs registration for given event handler in processing list
   *
   * @param eventHandler event handler
   * @return true handlers is registered
   */
  public static <T extends AbstractEventHandler> boolean registerEventHandler(T eventHandler) {
    return eventProcessor.getEventHandlers().add(eventHandler);
  }

  /**
   * Clears the registry of event handlers.
   */
  public static void clearEventHandlers() {
    eventProcessor.getEventHandlers().clear();
  }
}
