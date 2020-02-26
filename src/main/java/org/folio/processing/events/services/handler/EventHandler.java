package org.folio.processing.events.services.handler;

import org.folio.DataImportEventPayload;

import java.util.concurrent.CompletableFuture;

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
  CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload context);

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
