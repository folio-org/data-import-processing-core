package org.folio.processing.events.handlers;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;

import java.util.concurrent.CompletableFuture;

/**
 * Test event handler. Returns future that is exceptionally completed (failed).
 */
public class FailExceptionallyHandler implements EventHandler {

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload eventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture();
    future.completeExceptionally(new IllegalArgumentException("Can not handle event payload"));
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload eventPayload) {
    return true;
  }
}
