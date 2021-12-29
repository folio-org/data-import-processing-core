package org.folio.processing.events.services.publisher;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.AbstractRestTest;
import org.folio.rest.jaxrs.model.Event;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.junit.Assert.assertFalse;

@RunWith(VertxUnitRunner.class)
public class RestEventPublisherTest extends AbstractRestTest {
  public static final String PUBLISH_SERVICE_URL = "/pubsub/publish";

  private RestEventPublisher eventPublisher = new RestEventPublisher();

  @Test
  public void shouldPublishPayload() {
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_COMPLETED.value())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withContext(new HashMap<>() {{
        put("recordId", UUID.randomUUID().toString());
        put("chunkId", UUID.randomUUID().toString());
      }});

    WireMock.stubFor(WireMock.post(PUBLISH_SERVICE_URL).willReturn(WireMock.noContent()));
    CompletableFuture<Event> future = eventPublisher.publish(eventPayload);

    assertFalse(future.isCompletedExceptionally());
  }
}
