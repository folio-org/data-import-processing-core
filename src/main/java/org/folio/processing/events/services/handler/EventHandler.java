package org.folio.processing.events.services.handler;

import org.folio.DataImportEventPayload;

import java.util.concurrent.CompletableFuture;

/**
 * The core interface for event handlers
 */
public interface EventHandler {

  /**
   * Handles event, updates event type and event chain if necessary
   *
   * @param eventPayload event payload
   * @return future with event payload after handling
   */
  CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload eventPayload);

  /**
   * Checks whether event handler is eligible to handle specified event (based on Profile type and Entity type)
   *
   * @param eventPayload event payload
   * @return true if event handler is eligible to handle the event
   */
  boolean isEligible(DataImportEventPayload eventPayload);

  /**
   * Checks whether post-processing should be applied to event payload after handling by this handler
   *
   * @return true if event payload should pass post-processing
   */
  default boolean isPostProcessingNeeded() {
    return false;
  }

  /**
   * Returns event type to initiate post-processing of event payload by a handler in another module
   *
   * @return event type
   */
  default String getPostProcessingInitializationEventType() {
    throw new UnsupportedOperationException();
  }

}
