package org.folio.processing.events;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.MatchProfile;
import org.folio.processing.events.handlers.CreateHoldingsRecordEventHandler;
import org.folio.processing.events.handlers.CreateInstanceEventHandler;
import org.folio.processing.events.handlers.CreateItemRecordEventHandler;
import org.folio.processing.events.handlers.FailExceptionallyHandler;
import org.folio.processing.events.handlers.InstancePostProcessingEventHandler;
import org.folio.processing.events.handlers.UpdateInstanceEventHandler;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ReactTo.MATCH;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ReactTo.NON_MATCH;
import static org.mockito.ArgumentMatchers.any;

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
      .withProfileSnapshot(new ProfileSnapshotWrapper()
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
                .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM))))))))
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
      .withProfileSnapshot(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM))))
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
  public void shouldIgnoreEventIfNoHandlersDefined(TestContext testContext) {
    Async async = testContext.async();
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_HOLDINGS_RECORD_CREATED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withProfileSnapshot(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM))))
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM))));
    // when
    EventManager.handleEvent(eventPayload).whenComplete((nextEventContext, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertEquals(0, eventPayload.getEventsChain().size());
      testContext.assertEquals("DI_HOLDINGS_RECORD_CREATED", eventPayload.getEventType());
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
      .withProfileSnapshot(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM))))
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
  public void shouldHandleFirstEventInJobProfile(TestContext testContext) {
    Async async = testContext.async();
    // given
    String jobProfileId = UUID.randomUUID().toString();
    String actionProfileId = UUID.randomUUID().toString();
    EventManager.registerEventHandler(new CreateInstanceEventHandler());
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_SRS_MARC_BIB_RECORD_CREATED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withProfileSnapshot(new ProfileSnapshotWrapper()
        .withId(jobProfileId)
        .withContentType(JOB_PROFILE)
        .withContent(JsonObject.mapFrom(new JobProfile()))
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withId(actionProfileId)
            .withContentType(ACTION_PROFILE)
            .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.INSTANCE))))));
    // when
    EventManager.handleEvent(eventPayload).whenComplete((eventContext, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertEquals(2, eventContext.getEventsChain().size());
      testContext.assertEquals(2, eventContext.getCurrentNodePath().size());
      testContext.assertEquals(
        eventContext.getCurrentNodePath(),
        Arrays.asList(jobProfileId, actionProfileId)
      );
      testContext.assertEquals(
        eventContext.getEventsChain(),
        Arrays.asList("DI_SRS_MARC_BIB_RECORD_CREATED", "DI_INVENTORY_INSTANCE_CREATED")
      );
      testContext.assertEquals("DI_COMPLETED", eventContext.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldHandleAndSetToCurrentNodeAction2Wrapper(TestContext testContext) {
    Async async = testContext.async();
    // given
    CreateInstanceEventHandler createInstanceHandler = Mockito.spy(new CreateInstanceEventHandler());
    Mockito.doAnswer(invocationOnMock -> {
      DataImportEventPayload payload = invocationOnMock.getArgument(0);
      payload.setCurrentNode(payload.getCurrentNode().getChildSnapshotWrappers().get(0));
      return invocationOnMock.callRealMethod();
    }).when(createInstanceHandler).handle(any(DataImportEventPayload.class));

    EventManager.registerEventHandler(createInstanceHandler);

    ProfileSnapshotWrapper mapping1Wrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(MAPPING_PROFILE)
      .withContent(JsonObject.mapFrom(new MappingProfile()
        .withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(INSTANCE)));

    ProfileSnapshotWrapper mapping2Wrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(MAPPING_PROFILE)
      .withContent(JsonObject.mapFrom(new MappingProfile()
        .withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(HOLDINGS)));

    ProfileSnapshotWrapper action1Wrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withReactTo(NON_MATCH)
      .withOrder(0)
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.INSTANCE)))
      .withChildSnapshotWrappers(Collections.singletonList(mapping1Wrapper));

    ProfileSnapshotWrapper action2Wrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withReactTo(NON_MATCH)
      .withOrder(1)
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.HOLDINGS)))
      .withChildSnapshotWrappers(Collections.singletonList(mapping2Wrapper));

    ProfileSnapshotWrapper jobProfileWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()))
      .withChildSnapshotWrappers(Collections.singletonList(
        new ProfileSnapshotWrapper()
          .withId(UUID.randomUUID().toString())
          .withContentType(MATCH_PROFILE)
          .withContent(JsonObject.mapFrom(new MatchProfile().withIncomingRecordType(INSTANCE).withExistingRecordType(MARC_BIBLIOGRAPHIC)))
          .withChildSnapshotWrappers(Arrays.asList(action1Wrapper, action2Wrapper))));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_NOT_MATCHED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withProfileSnapshot(jobProfileWrapper)
      .withCurrentNode(action1Wrapper);

    // when
    EventManager.handleEvent(eventPayload).whenComplete((eventContext, throwable) -> {
    // then
      testContext.assertNull(throwable);
      testContext.assertEquals(action2Wrapper.getId(), eventContext.getCurrentNode().getId());
      testContext.assertEquals(1, eventContext.getEventsChain().size());
      testContext.assertEquals(
        Collections.singletonList(DI_INVENTORY_INSTANCE_NOT_MATCHED.value()),
        eventContext.getEventsChain()
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_CREATED.value(), eventContext.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldHandleAndSetToCurrentNodeAction1Wrapper(TestContext testContext) {
    Async async = testContext.async();
    // given
    EventHandler matchInstanceHandler = Mockito.mock(EventHandler.class);
    Mockito.doAnswer(invocationOnMock -> {
      DataImportEventPayload payload = invocationOnMock.getArgument(0);
      return CompletableFuture.completedFuture(payload.withEventType(DI_INVENTORY_INSTANCE_NOT_MATCHED.value()));
    }).when(matchInstanceHandler).handle(any(DataImportEventPayload.class));
    Mockito.when(matchInstanceHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);

    EventManager.registerEventHandler(matchInstanceHandler);

    ProfileSnapshotWrapper action1Wrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withReactTo(NON_MATCH)
      .withOrder(0)
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.INSTANCE)));

    ProfileSnapshotWrapper action2Wrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withReactTo(MATCH)
      .withOrder(0)
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.INSTANCE)));

    ProfileSnapshotWrapper matchWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(new MatchProfile().withIncomingRecordType(INSTANCE).withExistingRecordType(MARC_BIBLIOGRAPHIC)));

    ProfileSnapshotWrapper jobProfileWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()))
      .withChildSnapshotWrappers(Collections.singletonList(
        matchWrapper.withChildSnapshotWrappers(Arrays.asList(action1Wrapper, action2Wrapper))));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withProfileSnapshot(jobProfileWrapper)
      .withCurrentNode(matchWrapper);

    // when
    EventManager.handleEvent(eventPayload).whenComplete((eventContext, throwable) -> {
    // then
      testContext.assertNull(throwable);
      testContext.assertEquals(action1Wrapper.getId(), eventContext.getCurrentNode().getId());
      testContext.assertEquals(DI_INVENTORY_INSTANCE_NOT_MATCHED.value(), eventContext.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldHandleEventAndPreparePayloadForPostProcessing(TestContext testContext) {
    Async async = testContext.async();
    // given
    String jobProfileId = UUID.randomUUID().toString();
    String actionProfileId = UUID.randomUUID().toString();
    EventManager.registerEventHandler(new UpdateInstanceEventHandler());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withProfileSnapshot(new ProfileSnapshotWrapper()
        .withId(jobProfileId)
        .withContentType(JOB_PROFILE)
        .withContent(JsonObject.mapFrom(new JobProfile()))
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withId(actionProfileId)
            .withContentType(ACTION_PROFILE)
            .withContent(JsonObject.mapFrom(new ActionProfile().withAction(UPDATE).withFolioRecord(ActionProfile.FolioRecord.INSTANCE))))));
    // when
    EventManager.handleEvent(eventPayload).whenComplete((payload, throwable) -> {
    // then
      testContext.assertNull(throwable);
      HashMap<String, String> context = payload.getContext();
      testContext.assertEquals(UpdateInstanceEventHandler.POST_PROC_INIT_EVENT, payload.getEventType());
      testContext.assertEquals(UpdateInstanceEventHandler.POST_PROC_RESULT_EVENT, context.get(EventManager.POST_PROCESSING_RESULT_EVENT_KEY));

      testContext.assertEquals(1, payload.getEventsChain().size());
      testContext.assertEquals(1, payload.getCurrentNodePath().size());
      testContext.assertEquals(payload.getCurrentNodePath(), Collections.singletonList(jobProfileId));
      testContext.assertEquals(payload.getEventsChain(), Collections.singletonList(DI_SRS_MARC_BIB_RECORD_CREATED.value()));
      async.complete();
    });
  }

  @Test
  public void shouldPerformEventPostProcessingAndPreparePayloadAfterPostProcessing(TestContext testContext) {
    Async async = testContext.async();
    // given
    String jobProfileId = UUID.randomUUID().toString();
    String actionProfileId = UUID.randomUUID().toString();
    EventManager.registerEventHandler(new InstancePostProcessingEventHandler());


    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EventManager.POST_PROCESSING_RESULT_EVENT_KEY, UpdateInstanceEventHandler.POST_PROC_RESULT_EVENT);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(UpdateInstanceEventHandler.POST_PROC_INIT_EVENT)
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(payloadContext)
      .withProfileSnapshot(new ProfileSnapshotWrapper()
        .withId(jobProfileId)
        .withContentType(JOB_PROFILE)
        .withContent(JsonObject.mapFrom(new JobProfile()))
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withId(actionProfileId)
            .withContentType(ACTION_PROFILE)
            .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.INSTANCE))))));
    // when
    EventManager.handleEvent(eventPayload).whenComplete((payload, throwable) -> {
    // then
      testContext.assertNull(throwable);
      HashMap<String, String> context = payload.getContext();
      testContext.assertEquals(DI_COMPLETED.value(), payload.getEventType());
      testContext.assertNull(context.get(EventManager.POST_PROCESSING_RESULT_EVENT_KEY));

      testContext.assertEquals(2, payload.getEventsChain().size());
      testContext.assertEquals(2, payload.getCurrentNodePath().size());
      testContext.assertEquals(payload.getCurrentNodePath(), Arrays.asList(jobProfileId, actionProfileId));
      testContext.assertEquals(payload.getEventsChain(),
        Arrays.asList(UpdateInstanceEventHandler.POST_PROC_INIT_EVENT, UpdateInstanceEventHandler.POST_PROC_RESULT_EVENT));
      async.complete();
    });
  }
}
