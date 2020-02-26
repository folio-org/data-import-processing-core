package org.folio.processing.events.handlers;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.AbstractEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Test event handler. Handles event context with event CREATED_HOLDINGS_RECORD
 */
public class CreateItemRecordEventHandler extends AbstractEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(CreateItemRecordEventHandler.class);

  @Override
  public CompletableFuture<DataImportEventPayload> handleContext(DataImportEventPayload context) {
    LOGGER.info("Handling event " + getHandlerEventType());
    return CompletableFuture.completedFuture(context);
  }

  @Override
  public String getHandlerEventType() {
    return "DI_HOLDINGS_RECORD_CREATED";
  }

  @Override
  public String getTargetEventType() {
    return "DI_ITEM_RECORD_CREATED";
  }
}
