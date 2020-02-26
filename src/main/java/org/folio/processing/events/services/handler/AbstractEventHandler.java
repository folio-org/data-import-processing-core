package org.folio.processing.events.services.handler;

import org.folio.DataImportEventPayload;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract class to handle events for profiles
 */
public abstract class AbstractEventHandler implements EventHandler {

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload eventPayload) {
    return handleContext(eventPayload)
      .thenCompose(nextContext -> CompletableFuture.completedFuture(prepareForNextHandler(nextContext)));
  }

  protected abstract CompletableFuture<DataImportEventPayload> handleContext(DataImportEventPayload eventPayload);

  protected DataImportEventPayload prepareForNextHandler(DataImportEventPayload eventPayload) {
    eventPayload.setEventType(getTargetEventType());
    eventPayload.getEventsChain().add(getHandlerEventType());
    return eventPayload;
  }
}
