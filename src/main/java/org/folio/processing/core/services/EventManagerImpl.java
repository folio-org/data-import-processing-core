package org.folio.processing.core.services;

import io.vertx.core.Future;
import org.folio.processing.core.model.EventContext;
import org.folio.processing.core.services.processor.EventProcessor;

public class EventManagerImpl implements EventManager {
  private EventProcessor eventProcessor;

  public EventManagerImpl(EventProcessor eventProcessor) {
    this.eventProcessor = eventProcessor;
  }

  @Override
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
    return context;
  }
}
