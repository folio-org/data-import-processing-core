package org.folio.processing.core.services.processor;

import io.vertx.core.Future;
import org.folio.processing.core.services.handler.EventHandler;
import org.folio.processing.core.model.Context;

import java.util.List;
import java.util.Optional;

public class RecursiveEventProcessor implements EventProcessor {
  private List<EventHandler> eventHandlers;

  public RecursiveEventProcessor(List<EventHandler> eventHandlers) {
    this.eventHandlers = eventHandlers;
  }

  @Override
  public Future<Context> process(Context context) {
    Future<Context> future = Future.future();
    String eventType = context.getEventType();
    Optional<EventHandler> optionalEventHandler = eventHandlers.stream()
      .filter(eventHandler -> eventHandler.getEventType().equals(eventType))
      .findFirst();
    if (optionalEventHandler.isPresent()) {
      context.setHandled(true);
      EventHandler nextEventHandler = optionalEventHandler.get();
      processEventRecursively(context, nextEventHandler).setHandler(future);
    } else {
      context.setHandled(false);
      future.complete(context);
    }
    return future;
  }

  private Future<Context> processEventRecursively(Context context, EventHandler eventHandler) {
    Future<Context> future = Future.future();
    eventHandler.handle(context).setHandler(ar -> {
      if (ar.failed()) {
        future.fail(ar.cause());
      } else {
        String eventType = context.getEventType();
        Optional<EventHandler> optionalEventHandler = eventHandlers.stream()
          .filter(nextEventHandler -> nextEventHandler.getEventType().equals(eventType))
          .findFirst();
        if (optionalEventHandler.isPresent()) {
          EventHandler nextEventHandler = optionalEventHandler.get();
          processEventRecursively(context, nextEventHandler).setHandler(future);
        } else {
          future.complete(context);
        }
      }
    });
    return future;
  }
}
