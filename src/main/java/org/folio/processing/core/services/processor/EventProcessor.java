package org.folio.processing.core.services.processor;

import io.vertx.core.Future;
import org.folio.processing.core.model.EventContext;
import org.folio.processing.core.services.handler.EventHandler;

/**
 * The central interface for event processors.
 */
public interface EventProcessor {

  /**
   * Performs event processing
   *
   * @param context event context
   * @return future with event context
   */
  Future<EventContext> process(EventContext context);

  /**
   * Adds handler
   *
   * @param eventHandler event handler
   * @return true if handler is registered
   */
  boolean addHandler(EventHandler eventHandler);
}
