package org.folio.processing.events.services.publisher;

import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.tools.PomReader;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.util.pubsub.PubSubClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class RestEventPublisher implements EventPublisher {

  private static final Logger LOGGER = LoggerFactory.getLogger(RestEventPublisher.class);

  @Override
  public CompletableFuture<Event> publish(DataImportEventPayload eventPayload) {
    CompletableFuture<Event> future = new CompletableFuture<>();
    try {
      OkapiConnectionParams params = new OkapiConnectionParams();
      params.setOkapiUrl(eventPayload.getOkapiUrl());
      params.setTenantId(eventPayload.getTenant());
      params.setToken(eventPayload.getToken());

      Event event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventPayload.getEventType())
        .withEventPayload(ZIPArchiver.zip(JsonObject.mapFrom(eventPayload).encode()))
        .withEventMetadata(new EventMetadata()
          .withTenantId(params.getTenantId())
          .withEventTTL(1)
          .withPublishedBy(PomReader.INSTANCE.getModuleName() + "-" + PomReader.INSTANCE.getVersion()));

      PubSubClientUtils.sendEventMessage(event, params).whenComplete((published, throwable) -> {
        if (throwable != null) {
          LOGGER.error("Error publishing event");
          future.completeExceptionally(throwable);
        } else {
          LOGGER.info("Event has been published");
          future.complete(event);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Can not publish event", e);
      future.completeExceptionally(e);
    }
    return future;
  }
}
