package org.folio.processing.core.services.handler;

import io.vertx.core.Future;
import org.folio.processing.core.model.Context;

/**
 *
 */
public abstract class AbstractEventHandler implements EventHandler {

  @Override
  public Future<Context> handle(Context context) {
    Future<Context> future = Future.future();
    handleContext(context)
      .compose(nextContext -> Future.succeededFuture(prepareForNextHandler(nextContext)))
      .setHandler(future);
    return future;
  }

  protected abstract Future<Context> handleContext(Context context);

  protected Context prepareForNextHandler(Context context) {
    context.setEventType(getNextEventType());
    context.getEventChain().add(getEventType());
    return context;
  }
}
