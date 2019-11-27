package org.folio.processing.events.handlers;

import org.folio.processing.events.model.EventContext;
import org.folio.processing.events.services.handler.AbstractEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Test event handler. Handles event context with event CREATED_SRS_MARC_BIB_RECORD
 */
public class CreateInstanceEventHandler extends AbstractEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(CreateInstanceEventHandler.class);

  @Override
  public CompletableFuture<EventContext> handleContext(EventContext context) {
    LOGGER.info("Handling event " + getHandlerEventType());
    return CompletableFuture.completedFuture(context);
  }

  @Override
  public String getHandlerEventType() {
    return "CREATED_SRS_MARC_BIB_RECORD";
  }

  @Override
  public String getTargetEventType() {
    return "CREATED_INVENTORY_INSTANCE";
  }
}
