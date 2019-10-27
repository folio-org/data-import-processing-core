package org.folio.processing.core.services.handler;

import io.vertx.core.Future;
import org.folio.processing.core.model.EventContext;

/**
 * Abstract class to handle events for profiles
 */
public abstract class AbstractEventHandler implements EventHandler {

  @Override
  public Future<EventContext> handle(EventContext context) {
    Future<EventContext> future = Future.future();
    handleContext(context)
      .compose(nextContext -> Future.succeededFuture(prepareForNextHandler(nextContext)))
      .setHandler(future);
    return future;
  }

  protected abstract Future<EventContext> handleContext(EventContext context);

  protected EventContext prepareForNextHandler(EventContext context) {
    context.setEventType(getTargetEventType());
    context.getEventChain().add(getHandlerEventType());
    return context;
  }
}
