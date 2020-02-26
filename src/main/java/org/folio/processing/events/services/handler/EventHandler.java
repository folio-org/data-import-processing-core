package org.folio.processing.events.services.handler;

import org.folio.DataImportEventPayload;

import java.util.concurrent.CompletableFuture;

/**
 * The core interface for event handlers
 */
public interface EventHandler {

  /**
   * Handles event
   *
   * @param eventPayload event payload
   * @return future with event payload
   */
  CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload eventPayload);

  /**
   * Returns event type that handler can handle.
   * <code>handle</code> methods runs if type of event from context is the same as type of handler.
   *
   * @return handler event type
   */
  String getHandlerEventType();

  /**
   * Returns event type that handler sets to DataImportEventPayload as a result of handling.
   *
   * @return target event type
   */
  String getTargetEventType();
}
