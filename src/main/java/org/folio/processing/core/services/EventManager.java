package org.folio.processing.core.services;

import io.vertx.core.Future;
import org.folio.processing.core.model.Context;

public interface EventManager {

  /**
   * @param event
   * @return
   */
  Future<Context> handleEvent(Context event);
}
