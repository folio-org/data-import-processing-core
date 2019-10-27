package org.folio.processing.core.services.handler;

import io.vertx.core.Future;
import org.folio.processing.core.model.Context;

public interface EventHandler {

  /**
   * @param context
   * @return
   */
  Future<Context> handle(Context context);

  /**
   * @return
   */
  String getEventType();

  /**
   * @return
   */
  String getNextEventType();
}
