package org.folio.processing.events.utils;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.EncodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.AbstractRestTest;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.rest.util.RestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class RestUtilTest extends AbstractRestTest {
  public static final String PUBLISH_SERVICE_URL = "/pubsub/publish";

  private static OkapiConnectionParams params = new OkapiConnectionParams();
  private DataImportEventPayload eventPayload = new DataImportEventPayload()
    .withEventType(DI_COMPLETED.value())
    .withOkapiUrl(OKAPI_URL)
    .withTenant(TENANT_ID)
    .withToken(TOKEN)
    .withContext(new HashMap<>() {{
      put("recordId", UUID.randomUUID().toString());
      put("chunkId", UUID.randomUUID().toString());
    }});

  @Before
  public void prepareParams() {
    params.setOkapiUrl(OKAPI_URL);
    params.setTenantId(TENANT_ID);
    params.setToken(TOKEN);
  }

  @Test
  public void shouldReturnNoContentStatus(TestContext testContext) throws IOException {
    Async async = testContext.async();
    WireMock.stubFor(WireMock.post(PUBLISH_SERVICE_URL).willReturn(WireMock.noContent()));

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventPayload.getEventType())
      .withEventPayload(ZIPArchiver.zip(JsonObject.mapFrom(eventPayload).encode()))
      .withEventMetadata(new EventMetadata()
        .withTenantId(params.getTenantId())
        .withEventTTL(1)
        .withPublishedBy(PomReaderUtil.INSTANCE.constructModuleVersionAndVersion(PomReaderUtil.INSTANCE.getModuleName(), PomReaderUtil.INSTANCE.getVersion())));

    Promise<Event> promise = Promise.promise();
    RestUtil.doRequest(params, "/pubsub/publish", HttpMethod.POST, event)
      .onComplete(postPublishResult -> {
        if(RestUtil.validateAsyncResult(postPublishResult, promise)) {
          Assert.assertEquals(204, postPublishResult.result().getCode());
        } else {
          fail();
        }
        async.complete();
      });
  }

  @Test
  public void shouldReturnNotFoundStatus(TestContext testContext) throws IOException {
    Async async = testContext.async();
    WireMock.stubFor(WireMock.post(PUBLISH_SERVICE_URL).willReturn(WireMock.notFound()));

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventPayload.getEventType())
      .withEventPayload(ZIPArchiver.zip(JsonObject.mapFrom(eventPayload).encode()))
      .withEventMetadata(new EventMetadata()
        .withTenantId(params.getTenantId())
        .withEventTTL(1)
        .withPublishedBy(PomReaderUtil.INSTANCE.constructModuleVersionAndVersion(PomReaderUtil.INSTANCE.getModuleName(), PomReaderUtil.INSTANCE.getVersion())));

    Promise<Event> promise = Promise.promise();
    RestUtil.doRequest(params, "/pubsub/publish", HttpMethod.POST, event)
      .onComplete(postPublishResult -> {
        if(RestUtil.validateAsyncResult(postPublishResult, promise)) {
          fail();
        } else {
          Assert.assertEquals(404, postPublishResult.result().getCode());
        }
        async.complete();
      });
  }

  @Test
  public void shouldReturnInternalServerError(TestContext testContext) throws IOException {
    Async async = testContext.async();
    WireMock.stubFor(WireMock.post(PUBLISH_SERVICE_URL).willReturn(WireMock.serverError()));

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventPayload.getEventType())
      .withEventPayload(ZIPArchiver.zip(JsonObject.mapFrom(eventPayload).encode()))
      .withEventMetadata(new EventMetadata()
        .withTenantId(params.getTenantId())
        .withEventTTL(1)
        .withPublishedBy(PomReaderUtil.INSTANCE.constructModuleVersionAndVersion(PomReaderUtil.INSTANCE.getModuleName(), PomReaderUtil.INSTANCE.getVersion())));

    Promise<Event> promise = Promise.promise();
    RestUtil.doRequest(params, "/pubsub/publish", HttpMethod.POST, event)
      .onComplete(postPublishResult -> {
        if(RestUtil.validateAsyncResult(postPublishResult, promise)) {
          fail();
        } else {
          Assert.assertEquals(500, postPublishResult.result().getCode());
        }
        async.complete();
      });
  }

  @Test
  public void shouldReturnForbiddenStatus(TestContext testContext) throws IOException {
    Async async = testContext.async();
    WireMock.stubFor(WireMock.post(PUBLISH_SERVICE_URL).willReturn(WireMock.forbidden()));

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventPayload.getEventType())
      .withEventPayload(ZIPArchiver.zip(JsonObject.mapFrom(eventPayload).encode()))
      .withEventMetadata(new EventMetadata()
        .withTenantId(params.getTenantId())
        .withEventTTL(1)
        .withPublishedBy(PomReaderUtil.INSTANCE.constructModuleVersionAndVersion(PomReaderUtil.INSTANCE.getModuleName(), PomReaderUtil.INSTANCE.getVersion())));

    Promise<Event> promise = Promise.promise();
    RestUtil.doRequest(params, "/pubsub/publish", HttpMethod.POST, event)
      .onComplete(postPublishResult -> {
        if(RestUtil.validateAsyncResult(postPublishResult, promise)) {
          fail();
        } else {
          Assert.assertEquals(403, postPublishResult.result().getCode());
        }
        async.complete();
      });
  }

  @Test
  public void shouldReturnFailedFutureWhenJsonDoesntParse(TestContext testContext) {
    Async async = testContext.async();
    WireMock.stubFor(WireMock.post(PUBLISH_SERVICE_URL).willReturn(WireMock.forbidden()));

    Object mockItem = mock(Object.class);
    when(mockItem.toString()).thenReturn(mockItem.getClass().getName());

    RestUtil.doRequest(params, "/pubsub/publish", HttpMethod.POST, mockItem)
      .onComplete(postPublishResult -> {
        testContext.assertTrue(postPublishResult.failed());
        Throwable throwable = postPublishResult.cause();
        testContext.assertTrue(throwable instanceof EncodeException);
        async.complete();
      });
  }
}
