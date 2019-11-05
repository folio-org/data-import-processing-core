package org.folio.processing.core.services.handler;

import io.vertx.core.Future;
import org.folio.processing.core.model.EventContext;

/**
 * The core interface for event handlers
 */
public interface EventHandler {

  /**
   * Handles context
   *
   * @param context event context
   * @return future with context
   */
  Future<EventContext> handle(EventContext context);

  /**
   * Returns event type that handler can handle.
   * <code>handle</code> methods runs if type of event from context is the same as type of handler.
   *
   * @return handler event type
   */
  String getHandlerEventType();

  /**
   * Returns event type that handler sets to EventContext as a result of handling.
   *
   * @return target event type
   */
  String getTargetEventType();
}
