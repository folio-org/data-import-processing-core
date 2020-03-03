package org.folio.processing.events.handlers;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Test event handler. Returns future that is exceptionally completed (failed).
 */
public class FailExceptionallyHandler implements EventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(FailExceptionallyHandler.class);

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload eventPayload) {
    CompletableFuture future = new CompletableFuture();
    future.completeExceptionally(new IllegalArgumentException("Can not handle event payload"));
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload eventPayload) {
    return false;
  }
}
