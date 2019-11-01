package org.folio.processing.core;

import io.vertx.core.Future;
import org.folio.processing.core.model.EventContext;
import org.folio.processing.core.services.handler.AbstractEventHandler;
import org.folio.processing.core.services.processor.EventProcessor;
import org.folio.processing.core.services.processor.RecursiveEventProcessor;
import org.folio.processing.core.services.publisher.EventPublisher;
import org.folio.processing.core.services.publisher.RestEventPublisher;
import org.folio.processing.core.util.EventContextUtil;

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
  public static Future<Void> handleEvent(String eventPayload) {
    Future<Void> future = Future.future();
    EventContext eventContext = EventContextUtil.fromEventPayload(eventPayload);
    eventProcessor.process(eventContext).setHandler(firstAr -> {
      if (firstAr.failed()) {
        future.fail(firstAr.cause());
      } else {
        if (eventContext.isHandled()) {
          prepareContext(eventContext);
          eventPublisher.publish(eventContext).setHandler(future);
        } else {
          future.complete();
        }
      }
    });
    return future;
  }

  /**
   * Prepares given event context for publishing.
   *
   * @param context event context
   */
  private static void prepareContext(EventContext context) {
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
   * Flushes a registry of event handlers.
   */
  public static void clearEventHandlers() {
    eventProcessor.getEventHandlers().clear();
  }
}
