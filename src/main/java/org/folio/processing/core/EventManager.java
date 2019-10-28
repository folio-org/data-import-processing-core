package org.folio.processing.core;

import io.vertx.core.Future;
import org.folio.processing.core.model.EventContext;
import org.folio.processing.core.services.handler.EventHandler;
import org.folio.processing.core.services.processor.EventProcessor;
import org.folio.processing.core.services.processor.RecursiveEventProcessor;

public class EventManager {

  private EventProcessor eventProcessor = new RecursiveEventProcessor();

  /**
   * Handles event
   *
   * @param context event context
   * @return future with event context
   */
  public Future<EventContext> handleEvent(EventContext context) {
    Future<EventContext> future = Future.future();
    eventProcessor.process(context).setHandler(firstAr -> {
      if (firstAr.failed()) {
        future.fail(firstAr.cause());
      } else {
        if (context.isHandled()) {
          future.complete(prepareContext(context));
        } else {
          future.complete();
        }
      }
    });
    return future;
  }

  private EventContext prepareContext(EventContext context) {
    // update currentNode
    // update currentNodePath
    return context;
  }

  /**
   * Performs registration for given event handler in processing list
   *
   * @param eventHandler event handler
   * @return true handlers is registered
   */
  public boolean registerHandler(EventHandler eventHandler) {
    return eventProcessor.addHandler(eventHandler);
  }
}
