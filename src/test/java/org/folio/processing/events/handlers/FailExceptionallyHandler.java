package org.folio.processing.events.handlers;

import org.folio.DataImportEventPayload;
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
  public CompletableFuture<DataImportEventPayload> handleContext(DataImportEventPayload context) {
    LOGGER.info("Handling event " + getHandlerEventType());
    CompletableFuture future = new CompletableFuture();
    future.completeExceptionally(new IllegalArgumentException("Can not handle event context"));
    return future;
  }

  @Override
  public String getHandlerEventType() {
    return "DI_SRS_MARC_BIB_RECORD_CREATED";
  }

  @Override
  public String getTargetEventType() {
    return "UNDEFINED";
  }
}
