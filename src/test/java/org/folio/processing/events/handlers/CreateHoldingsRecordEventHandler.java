package org.folio.processing.events.handlers;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Test event handler. Handles event payload with event DI_INVENTORY_INSTANCE_CREATED
 */
public class CreateHoldingsRecordEventHandler implements EventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(CreateHoldingsRecordEventHandler.class);

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload eventPayload) {
    return CompletableFuture.completedFuture(eventPayload);  }

  @Override
  public boolean isEligible(DataImportEventPayload eventPayload) {
    return false;
  }
}
