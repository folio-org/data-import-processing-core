package org.folio.processing.events.services.publisher;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.impl.HttpStatusException;
import org.folio.DataImportEventPayload;
import org.folio.HttpStatus;
import org.folio.rest.client.PubsubClient;
import org.folio.rest.jaxrs.model.Event;
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
      Event event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventPayload.getEventType())
        .withEventPayload(JsonObject.mapFrom(eventPayload).encode());

      PubsubClient client = new PubsubClient(eventPayload.getOkapiUrl(), eventPayload.getTenant(), eventPayload.getToken());
      client.postPubsubPublish(event, response -> {
        if (response.statusCode() != HttpStatus.HTTP_NO_CONTENT.toInt()) {
          LOGGER.error("Error publishing event: received status code {}, {}", response.statusCode(), response.statusMessage());
          future.completeExceptionally(new HttpStatusException(response.statusCode(), "Error publishing event"));
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
