package org.folio.processing.events.services.publisher;

import io.vertx.ext.web.handler.impl.HttpStatusException;
import org.folio.HttpStatus;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.events.model.OkapiConnectionParams;
import org.folio.processing.events.util.EventContextUtil;
import org.folio.rest.client.PubsubClient;
import org.folio.rest.jaxrs.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class RestEventPublisher implements EventPublisher {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestEventPublisher.class);

  @Override
  public CompletableFuture<Event> publish(EventContext eventContext) {
    OkapiConnectionParams params = eventContext.getOkapiConnectionParams();
    String eventPayload = EventContextUtil.toEventPayload(eventContext);
    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventContext.getEventType())
      .withEventPayload(eventPayload);
    return postPubsubPublish(event, params);
  }

  /**
   * Sends event to mod-pubsub using REST client
   *
   * @param event  event
   * @param params connection parameters
   * @return future
   */
  private CompletableFuture<Event> postPubsubPublish(Event event, OkapiConnectionParams params) {
    CompletableFuture<Event> future = new CompletableFuture<>();
    PubsubClient client = new PubsubClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
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
