package org.folio.processing.events.services.publisher;

import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.utils.PomReaderUtil;
import org.folio.processing.events.utils.VertxUtils;
import org.folio.rest.util.RestUtil;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.util.OkapiConnectionParams;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

@Deprecated
public class RestEventPublisher implements EventPublisher {

  private static final Logger LOGGER = LogManager.getLogger(RestEventPublisher.class);

  @Override
  public CompletableFuture<Event> publish(DataImportEventPayload eventPayload) {
    Promise<Event> promise = Promise.promise();
    try {
      OkapiConnectionParams params = new OkapiConnectionParams(getOkapiHeaders(eventPayload), VertxUtils.getVertxFromContextOrNew());
      Event event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventPayload.getEventType())
        .withEventPayload(ZIPArchiver.zip(JsonObject.mapFrom(eventPayload).encode()))
        .withEventMetadata(new EventMetadata()
          .withTenantId(params.getTenantId())
          .withEventTTL(1)
          .withPublishedBy(PomReaderUtil.INSTANCE.constructModuleVersionAndVersion(PomReaderUtil.INSTANCE.getModuleName(), PomReaderUtil.INSTANCE.getVersion())));

      RestUtil.doRequest(params, "/pubsub/publish", HttpMethod.POST, event)
        .onComplete(postPublishResult -> {
          if(RestUtil.validateAsyncResult(postPublishResult, promise)) {
            LOGGER.info("{} event has been published", event.getEventType());
            promise.complete(event);
          } else {
            LOGGER.error("Error publishing {} event", event.getEventType());
          }
        });
    } catch (Exception e) {
      LOGGER.error("Can not publish event", e);
      promise.fail(e);
    }
    return promise.future().toCompletionStage().toCompletableFuture();
  }

  private Map<String, String> getOkapiHeaders(DataImportEventPayload eventPayload) {
    Map<String, String> okapiHeaders = new HashMap<>();
    okapiHeaders.put(OKAPI_URL_HEADER, eventPayload.getOkapiUrl());
    okapiHeaders.put(OKAPI_TENANT_HEADER, eventPayload.getTenant());
    okapiHeaders.put(OKAPI_TOKEN_HEADER, eventPayload.getToken());
    return okapiHeaders;
  }
}
