package org.folio.processing.events.handlers;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;

import java.util.concurrent.CompletableFuture;

/**
 * Test event handler. Throws exception while handling event payload.
 */
public class ThrowExceptionHandler implements EventHandler {

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload eventPayload) {
    throw new IllegalArgumentException("Can not handle event payload");
  }

  @Override
  public boolean isEligible(DataImportEventPayload eventPayload) {
    return false;
  }
}
