package org.folio.processing.events.services.processor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;

/**
 * The central interface for event processors.
 */
public interface EventProcessor {

  /**
   * Performs event processing
   *
   * @param eventPayload event payload
   * @return future with event payload after handling
   */
  CompletableFuture<DataImportEventPayload> process(DataImportEventPayload eventPayload);

  /**
   * @return list of handlers
   */
  List<EventHandler> getEventHandlers();
}
