package org.folio.processing.services.handlers;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.processing.core.model.EventContext;
import org.folio.processing.core.services.handler.AbstractEventHandler;

public class CreateItemRecordEventHandler extends AbstractEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(CreateItemRecordEventHandler.class);

  @Override
  public Future<EventContext> handleContext(EventContext context) {
    LOGGER.info("Handling event " + getHandlerEventType());
    return Future.succeededFuture(context);
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
