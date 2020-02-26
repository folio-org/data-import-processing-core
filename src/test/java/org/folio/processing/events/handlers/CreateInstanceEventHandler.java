package org.folio.processing.events.handlers;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.AbstractEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Test event handler. Handles event context with event DI_SRS_MARC_BIB_RECORD_CREATED
 */
public class CreateInstanceEventHandler extends AbstractEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(CreateInstanceEventHandler.class);

  @Override
  public CompletableFuture<DataImportEventPayload> handleContext(DataImportEventPayload eventPayload) {
    LOGGER.info("Handling event " + getHandlerEventType());
    return CompletableFuture.completedFuture(eventPayload);
  }

  @Override
  public String getHandlerEventType() {
    return "DI_SRS_MARC_BIB_RECORD_CREATED";
  }

  @Override
  public String getTargetEventType() {
    return "DI_INVENTORY_INSTANCE_CREATED";
  }
}
