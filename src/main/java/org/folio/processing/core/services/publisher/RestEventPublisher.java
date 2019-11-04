package org.folio.processing.core.services.publisher;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.handler.impl.HttpStatusException;
import org.folio.HttpStatus;
import org.folio.processing.core.model.EventContext;
import org.folio.processing.core.model.OkapiConnectionParams;
import org.folio.processing.core.util.EventContextUtil;
import org.folio.rest.client.PubsubClient;
import org.folio.rest.jaxrs.model.Event;

import java.util.UUID;

public class RestEventPublisher implements EventPublisher {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestEventPublisher.class);

  @Override
  public Future<Void> publish(EventContext context) {
    OkapiConnectionParams params = context.getOkapiConnectionParams();
    String eventPayload = EventContextUtil.toEventPayload(context);
    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(context.getEventType())
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
  private Future<Void> postPubsubPublish(Event event, OkapiConnectionParams params) {
    Future<Void> future = Future.future();
    PubsubClient client = new PubsubClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.postPubsubPublish(event, response -> {
        if (response.statusCode() != HttpStatus.HTTP_NO_CONTENT.toInt()) {
          LOGGER.error("Error publishing event", response.statusCode(), response.statusMessage());
          future.fail(new HttpStatusException(response.statusCode(), "Error publishing event"));
        } else {
          LOGGER.info("Event has been published");
          future.complete();
        }
      });
    } catch (Exception e) {
      LOGGER.error("Can not publish event", e);
      future.fail(e);
    }
    return future;
  }
}
