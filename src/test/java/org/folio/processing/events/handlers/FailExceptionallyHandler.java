package org.folio.processing.events.handlers;

import org.folio.processing.events.model.EventContext;
import org.folio.processing.events.services.handler.AbstractEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Test event handler. Returns future that is exceptionally completed (failed).
 */
public class FailExceptionallyHandler extends AbstractEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(FailExceptionallyHandler.class);

  @Override
  public CompletableFuture<EventContext> handleContext(EventContext context) {
    LOGGER.info("Handling event " + getHandlerEventType());
    CompletableFuture future = new CompletableFuture();
    future.completeExceptionally(new IllegalArgumentException("Can not handle event context"));
    return future;
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
