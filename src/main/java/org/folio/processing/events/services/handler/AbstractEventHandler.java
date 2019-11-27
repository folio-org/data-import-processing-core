package org.folio.processing.events.services.handler;

import org.folio.processing.events.model.EventContext;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract class to handle events for profiles
 */
public abstract class AbstractEventHandler implements EventHandler {

  @Override
  public CompletableFuture<EventContext> handle(EventContext context) {
    return handleContext(context)
      .thenCompose(nextContext -> CompletableFuture.completedFuture(prepareForNextHandler(nextContext)));
  }

  protected abstract CompletableFuture<EventContext> handleContext(EventContext context);

  protected EventContext prepareForNextHandler(EventContext context) {
    context.setEventType(getTargetEventType());
    context.getEventChain().add(getHandlerEventType());
    return context;
  }
}
