package org.folio.processing.core.services.processor;

import io.vertx.core.Future;
import org.folio.processing.core.model.EventContext;
import org.folio.processing.core.services.handler.EventHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RecursiveEventProcessor implements EventProcessor {
  private List<EventHandler> eventHandlers = new ArrayList<>();

  @Override
  public Future<EventContext> process(EventContext context) {
    Future<EventContext> future = Future.future();
    String eventType = context.getEventType();
    Optional<EventHandler> optionalEventHandler = eventHandlers.stream()
      .filter(eventHandler -> eventHandler.getHandlerEventType().equals(eventType))
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

  private Future<EventContext> processEventRecursively(EventContext context, EventHandler eventHandler) {
    Future<EventContext> future = Future.future();
    eventHandler.handle(context).setHandler(ar -> {
      if (ar.failed()) {
        future.fail(ar.cause());
      } else {
        String eventType = context.getEventType();
        Optional<EventHandler> optionalEventHandler = eventHandlers.stream()
          .filter(nextEventHandler -> nextEventHandler.getHandlerEventType().equals(eventType))
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

  @Override
  public List<EventHandler> getEventHandlers() {
    return eventHandlers;
  }
}
