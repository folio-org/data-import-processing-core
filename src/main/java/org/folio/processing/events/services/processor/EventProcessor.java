package org.folio.processing.events.services.processor;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;

import java.util.List;
import java.util.concurrent.CompletableFuture;

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
  CompletableFuture<DataImportEventPayload> process(DataImportEventPayload context);

  /**
   * @return list of handlers
   */
  List<EventHandler> getEventHandlers();
}
