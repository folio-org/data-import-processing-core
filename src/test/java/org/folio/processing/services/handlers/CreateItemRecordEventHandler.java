package org.folio.processing.services.handlers;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.processing.core.model.Context;
import org.folio.processing.core.services.handler.AbstractEventHandler;

public class CreateItemRecordEventHandler extends AbstractEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(CreateItemRecordEventHandler.class);

  @Override
  public Future<Context> handleContext(Context context) {
    LOGGER.info("Handling event " + getEventType());
    return Future.succeededFuture(context);
  }

  @Override
  public String getEventType() {
    return "CREATED_HOLDINGS_RECORD";
  }

  @Override
  public String getNextEventType() {
    return "CREATED_ITEM_RECORD";
  }
}
