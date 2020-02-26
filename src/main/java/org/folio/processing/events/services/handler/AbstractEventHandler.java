package org.folio.processing.events.services.handler;

import org.folio.DataImportEventPayload;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract class to handle events for profiles
 */
public abstract class AbstractEventHandler implements EventHandler {

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload context) {
    return handleContext(context)
      .thenCompose(nextContext -> CompletableFuture.completedFuture(prepareForNextHandler(nextContext)));
  }

  protected abstract CompletableFuture<DataImportEventPayload> handleContext(DataImportEventPayload context);

  protected DataImportEventPayload prepareForNextHandler(DataImportEventPayload context) {
    context.setEventType(getTargetEventType());
    context.getEventsChain().add(getHandlerEventType());
    return context;
  }
}
