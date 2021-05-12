package org.folio.processing.events.handlers;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;

import java.util.concurrent.CompletableFuture;

import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;

/**
 * Test event handler. Handles event payload with event DI_SRS_MARC_AUTHORITY_RECORD_CREATED
 */
public class CreateAuthorityEventHandler implements EventHandler {

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload eventPayload) {
    eventPayload.getEventsChain().add(eventPayload.getEventType());
    eventPayload.setEventType("DI_SRS_MARC_AUTHORITY_RECORD_CREATED");
    return CompletableFuture.completedFuture(eventPayload);
  }

  @Override
  public boolean isEligible(DataImportEventPayload eventPayload) {
    return eventPayload.getContext().containsKey(MARC_AUTHORITY.value());
  }
}
