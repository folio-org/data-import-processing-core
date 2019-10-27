package org.folio.processing.core.services;

import io.vertx.core.Future;
import org.folio.processing.core.model.EventContext;

/**
 * Service to manager events.
 */
public interface EventManager {

  /**
   * Handles event
   *
   * @param context event context
   * @return future with event context
   */
  Future<EventContext> handleEvent(EventContext context);
}
