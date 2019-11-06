package org.folio.processing.services.handlers;

import org.folio.processing.core.model.EventContext;
import org.folio.processing.core.services.handler.AbstractEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Test event handler. Handles event context with event CREATED_HOLDINGS_RECORD
 */
public class CreateItemRecordEventHandler extends AbstractEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(CreateItemRecordEventHandler.class);

  @Override
  public CompletableFuture<EventContext> handleContext(EventContext context) {
    LOGGER.info("Handling event " + getHandlerEventType());
    return CompletableFuture.completedFuture(context);
  }

  @Override
  public String getHandlerEventType() {
    return "CREATED_HOLDINGS_RECORD";
  }

  @Override
  public String getTargetEventType() {
    return "CREATED_ITEM_RECORD";
  }
}
