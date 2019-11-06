package org.folio.processing.services.handlers;

import org.folio.processing.core.model.EventContext;
import org.folio.processing.core.services.handler.AbstractEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Test event handler. Throws exception while handling event context.
 */
public class ThrowExceptionHandler extends AbstractEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(ThrowExceptionHandler.class);

  @Override
  public CompletableFuture<EventContext> handleContext(EventContext context) {
    LOGGER.info("Handling event " + getHandlerEventType());
    throw new IllegalArgumentException("Can not handle event context");
  }

  @Override
  public String getHandlerEventType() {
    return "CREATED_SRS_MARC_BIB_RECORD";
  }

  @Override
  public String getTargetEventType() {
    return "UNDEFINED";
  }
}
