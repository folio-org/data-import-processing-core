package org.folio.processing.events.handlers;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.AbstractEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Test event handler. Throws exception while handling event context.
 */
public class ThrowExceptionHandler extends AbstractEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(ThrowExceptionHandler.class);

  @Override
  public CompletableFuture<DataImportEventPayload> handleContext(DataImportEventPayload eventPayload) {
    LOGGER.info("Handling event " + getHandlerEventType());
    throw new IllegalArgumentException("Can not handle event payload");
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
