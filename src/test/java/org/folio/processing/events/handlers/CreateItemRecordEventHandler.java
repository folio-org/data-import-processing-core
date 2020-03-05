package org.folio.processing.events.handlers;

import io.vertx.core.json.JsonObject;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;

import java.util.concurrent.CompletableFuture;

import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

/**
 * Test event handler. Handles event context with event DI_HOLDINGS_RECORD_CREATED
 */
public class CreateItemRecordEventHandler implements EventHandler {

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload eventPayload) {
    eventPayload.getEventsChain().add(eventPayload.getEventType());
    eventPayload.setEventType("DI_ITEM_RECORD_CREATED");
    return CompletableFuture.completedFuture(eventPayload);
  }

  @Override
  public boolean isEligible(DataImportEventPayload eventPayload) {
    if (ACTION_PROFILE == eventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(eventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getFolioRecord() == ActionProfile.FolioRecord.ITEM;
    }
    return false;
  }
}
