package org.folio.processing.events.handlers;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.AbstractEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Test event handler. Handles event payload with event DI_INVENTORY_INSTANCE_CREATED
 */
public class CreateHoldingsRecordEventHandler extends AbstractEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(CreateHoldingsRecordEventHandler.class);

  @Override
  public CompletableFuture<DataImportEventPayload> handleEventPayload(DataImportEventPayload eventPayload) {
    LOGGER.info("Handling event " + getHandlerEventType());
    return CompletableFuture.completedFuture(eventPayload);
  }

  @Override
  public String getHandlerEventType() {
    return "DI_INVENTORY_INSTANCE_CREATED";
  }

  @Override
  public String getTargetEventType() {
    return "DI_HOLDINGS_RECORD_CREATED";
  }
}
