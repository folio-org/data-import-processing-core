package org.folio.processing.events;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.handlers.CreateHoldingsRecordEventHandler;
import org.folio.processing.events.handlers.CreateInstanceEventHandler;
import org.folio.processing.events.handlers.CreateItemRecordEventHandler;
import org.folio.processing.events.handlers.FailExceptionallyHandler;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

@RunWith(VertxUnitRunner.class)
public class EventManagerUnitTest extends AbstractRestTest {

  @Before
  public void beforeTest() {
    EventManager.clearEventHandlers();
  }

  @Test
  public void shouldHandleEvent(TestContext testContext) {
    Async async = testContext.async();
    // given
    EventManager.registerEventHandler(new CreateInstanceEventHandler());
    EventManager.registerEventHandler(new CreateHoldingsRecordEventHandler());
    EventManager.registerEventHandler(new CreateItemRecordEventHandler());
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_SRS_MARC_BIB_RECORD_CREATED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.INSTANCE)))
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withId(UUID.randomUUID().toString())
            .withContentType(ACTION_PROFILE)
            .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.HOLDINGS)))
            .withChildSnapshotWrappers(Collections.singletonList(
              new ProfileSnapshotWrapper()
                .withId(UUID.randomUUID().toString())
                .withContentType(ACTION_PROFILE)
                .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM))))))));
    // when
    EventManager.handleEvent(eventPayload).whenComplete((nextEventContext, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertEquals(1, nextEventContext.getEventsChain().size());
      testContext.assertEquals(
        nextEventContext.getEventsChain(),
        Collections.singletonList("DI_SRS_MARC_BIB_RECORD_CREATED")
      );
      testContext.assertEquals("DI_INVENTORY_INSTANCE_CREATED", nextEventContext.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldHandleLastEvent(TestContext testContext) {
    Async async = testContext.async();
    // given
    EventManager.registerEventHandler(new CreateInstanceEventHandler());
    EventManager.registerEventHandler(new CreateHoldingsRecordEventHandler());
    EventManager.registerEventHandler(new CreateItemRecordEventHandler());
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_HOLDINGS_RECORD_CREATED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM))));
    // when
    EventManager.handleEvent(eventPayload).whenComplete((nextEventContext, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertEquals(2, nextEventContext.getEventsChain().size());
      testContext.assertEquals(
        nextEventContext.getEventsChain(),
        Arrays.asList("DI_HOLDINGS_RECORD_CREATED", "DI_ITEM_RECORD_CREATED")
      );
      testContext.assertEquals("DI_COMPLETED", nextEventContext.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldHandleAsErrorEventIfNoHandlersDefined(TestContext testContext) {
    Async async = testContext.async();
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_HOLDINGS_RECORD_CREATED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM))));
    // when
    EventManager.handleEvent(eventPayload).whenComplete((nextEventContext, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertEquals(1, eventPayload.getEventsChain().size());
      testContext.assertEquals("DI_ERROR", eventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldHandleAsErrorEventIfHandlerCompletedExceptionally(TestContext testContext) {
    Async async = testContext.async();
    // given
    EventManager.registerEventHandler(new FailExceptionallyHandler());
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_HOLDINGS_RECORD_CREATED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM))));
    // when
    EventManager.handleEvent(eventPayload).whenComplete((nextEventContext, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertEquals(1, eventPayload.getEventsChain().size());
      testContext.assertEquals("DI_ERROR", eventPayload.getEventType());
      async.complete();
    });
  }
}
