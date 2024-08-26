package org.folio.processing.events;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.MatchProfile;
import org.folio.processing.events.handlers.CreateAuthorityEventHandler;
import org.folio.processing.events.handlers.CreateHoldingsRecordEventHandler;
import org.folio.processing.events.handlers.CreateInstanceEventHandler;
import org.folio.processing.events.handlers.CreateItemRecordEventHandler;
import org.folio.processing.events.handlers.FailExceptionallyHandler;
import org.folio.processing.events.handlers.InstancePostProcessingEventHandler;
import org.folio.processing.events.handlers.UpdateInstanceEventHandler;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.ReactToType.MATCH;
import static org.folio.rest.jaxrs.model.ReactToType.NON_MATCH;
import static org.mockito.ArgumentMatchers.any;

@RunWith(VertxUnitRunner.class)
public class EventManagerUnitTest extends AbstractRestTest {
  private final String PUBLISH_SERVICE_URL = "/pubsub/publish";

  @Before
  public void beforeTest() {
    EventManager.clearEventHandlers();
    WireMock.stubFor(WireMock.post(PUBLISH_SERVICE_URL).willReturn(WireMock.noContent()));
  }

  @Test
  public void shouldHandleEvent(TestContext testContext) {
    Async async = testContext.async();
    // given
    EventManager.registerEventHandler(new CreateInstanceEventHandler());
    EventManager.registerEventHandler(new CreateHoldingsRecordEventHandler());
    EventManager.registerEventHandler(new CreateItemRecordEventHandler());
    EventManager.registerEventHandler(new CreateAuthorityEventHandler());

    ProfileSnapshotWrapper profileSnapshot = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()))

      .withChildSnapshotWrappers(Collections.singletonList(
        new ProfileSnapshotWrapper()
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
                  .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM)))))))));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INCOMING_MARC_BIB_RECORD_PARSED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshot.getChildSnapshotWrappers().get(0));
    // when
    EventManager.handleEvent(eventPayload, profileSnapshot).whenComplete((nextEventContext, throwable) -> {
    // then
      testContext.assertNull(throwable);
      testContext.assertEquals(1, nextEventContext.getEventsChain().size());
      testContext.assertEquals(
        nextEventContext.getEventsChain(),
        Collections.singletonList("DI_INCOMING_MARC_BIB_RECORD_PARSED")
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
    EventManager.registerEventHandler(new CreateAuthorityEventHandler());

    ProfileSnapshotWrapper jobProfileSnapshot = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()))
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM)))));

        DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_HOLDINGS_RECORD_CREATED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(jobProfileSnapshot.getChildSnapshotWrappers().get(0));
    // when
    EventManager.handleEvent(eventPayload, jobProfileSnapshot).whenComplete((nextEventContext, throwable) -> {
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
    ProfileSnapshotWrapper profileSnapshot = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM)))));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_HOLDINGS_RECORD_CREATED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshot.getChildSnapshotWrappers().get(0));

    // when
    EventManager.handleEvent(eventPayload, profileSnapshot).whenComplete((nextEventContext, throwable) -> {
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

    ProfileSnapshotWrapper jobProfileSnapshot = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM)))));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_HOLDINGS_RECORD_CREATED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(jobProfileSnapshot.getChildSnapshotWrappers().get(0));
    // when
    EventManager.handleEvent(eventPayload, jobProfileSnapshot).whenComplete((nextEventContext, throwable) -> {
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

    ProfileSnapshotWrapper jobProfileSnapshot = new ProfileSnapshotWrapper()
      .withId(jobProfileId)
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()))
      .withChildSnapshotWrappers(Collections.singletonList(
        new ProfileSnapshotWrapper()
          .withId(actionProfileId)
          .withContentType(ACTION_PROFILE)
          .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.INSTANCE)))));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INCOMING_MARC_BIB_RECORD_PARSED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>());
    // when
    EventManager.handleEvent(eventPayload, jobProfileSnapshot).whenComplete((eventContext, throwable) -> {
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
        Arrays.asList("DI_INCOMING_MARC_BIB_RECORD_PARSED", "DI_INVENTORY_INSTANCE_CREATED")
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
      .withCurrentNode(action1Wrapper);

    // when
    EventManager.handleEvent(eventPayload, jobProfileWrapper).whenComplete((eventContext, throwable) -> {
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
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(matchWrapper);

    // when
    EventManager.handleEvent(eventPayload, jobProfileWrapper).whenComplete((eventContext, throwable) -> {
    // then
      testContext.assertNull(throwable);
      testContext.assertEquals(action1Wrapper.getId(), eventContext.getCurrentNode().getId());
      testContext.assertEquals(DI_INVENTORY_INSTANCE_NOT_MATCHED.value(), eventContext.getEventType());
      async.complete();
    });
  }

  @Test
  @Ignore
  public void shouldHandleAndSwitchNodes(TestContext testContext) {
    Async async = testContext.async();
    // given
    EventHandler updateInstanceHandler = Mockito.mock(EventHandler.class);
    Mockito.doAnswer(invocationOnMock -> {

      DataImportEventPayload payload = invocationOnMock.getArgument(0);
      payload.setCurrentNode(payload.getCurrentNode().getChildSnapshotWrappers().get(0));
      return CompletableFuture.completedFuture(payload.withEventType(DI_INVENTORY_INSTANCE_UPDATED.value()));

    }).when(updateInstanceHandler).handle(any(DataImportEventPayload.class));
    Mockito.when(updateInstanceHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);

    EventManager.registerEventHandler(updateInstanceHandler);

    // update instance
    ProfileSnapshotWrapper instanceUpdateMappingWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(0)
      .withContentType(MAPPING_PROFILE)
      .withContent(JsonObject.mapFrom(new MappingProfile()
        .withName("instanceUpdateMappingWrapper")
        .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
        .withExistingRecordType(INSTANCE)));

    ProfileSnapshotWrapper instanceUpdateActionWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withReactTo(MATCH)
      .withOrder(0)
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(new ActionProfile().withName("instanceUpdateActionWrapper").withFolioRecord(ActionProfile.FolioRecord.INSTANCE).withAction(UPDATE)))
      .withChildSnapshotWrappers(Collections.singletonList(instanceUpdateMappingWrapper));

    ProfileSnapshotWrapper instanceUpdateActionWrapper2 = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withReactTo(MATCH)
      .withOrder(0)
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(new ActionProfile().withName("instanceUpdateActionWrapper2").withFolioRecord(ActionProfile.FolioRecord.INSTANCE).withAction(UPDATE)))
      .withChildSnapshotWrappers(Collections.singletonList(instanceUpdateMappingWrapper));

    // create instance
    ProfileSnapshotWrapper instanceCreateMappingWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(0)
      .withContentType(MAPPING_PROFILE)
      .withContent(JsonObject.mapFrom(new MappingProfile().withName("instanceCreateMappingWrapper").withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(INSTANCE)));

    ProfileSnapshotWrapper instanceCreateActionWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withReactTo(NON_MATCH)
      .withOrder(0)
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(new ActionProfile().withName("instanceCreateActionWrapper").withFolioRecord(ActionProfile.FolioRecord.INSTANCE).withAction(CREATE)))
      .withChildSnapshotWrappers(Collections.singletonList(instanceCreateMappingWrapper));

    ProfileSnapshotWrapper instanceChildMatchWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(0)
      .withContentType(MATCH_PROFILE)
      .withReactTo(NON_MATCH)
      .withContent(JsonObject.mapFrom(new MatchProfile().withName("instanceChildMatchWrapper").withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(HOLDINGS)))
      .withChildSnapshotWrappers(List.of(instanceUpdateActionWrapper2, instanceCreateActionWrapper));

    ProfileSnapshotWrapper instanceParentMatchWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(0)
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(new MatchProfile().withName("instanceParentMatchWrapper").withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(INSTANCE)))
      .withChildSnapshotWrappers(List.of(instanceChildMatchWrapper, instanceUpdateActionWrapper));

    // update holdings
    ProfileSnapshotWrapper holdingsUpdateMappingWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(0)
      .withContentType(MAPPING_PROFILE)
      .withContent(JsonObject.mapFrom(new MappingProfile().withName("holdingsUpdateMappingWrapper").withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(HOLDINGS)));

    ProfileSnapshotWrapper holdingsUpdateActionWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withReactTo(MATCH)
      .withOrder(0)
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(new ActionProfile().withName("holdingsUpdateActionWrapper").withFolioRecord(ActionProfile.FolioRecord.HOLDINGS).withAction(UPDATE)))
      .withChildSnapshotWrappers(Collections.singletonList(holdingsUpdateMappingWrapper));

    // create holdings
    ProfileSnapshotWrapper holdingsCreateMappingWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(0)
      .withContentType(MAPPING_PROFILE)
      .withContent(JsonObject.mapFrom(new MappingProfile().withName("holdingsCreateMappingWrapper").withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(HOLDINGS)));

    ProfileSnapshotWrapper holdingsCreateActionWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withReactTo(NON_MATCH)
      .withOrder(0)
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(new ActionProfile().withName("holdingsCreateActionWrapper").withFolioRecord(ActionProfile.FolioRecord.HOLDINGS).withAction(CREATE)))
      .withChildSnapshotWrappers(Collections.singletonList(holdingsCreateMappingWrapper));

    ProfileSnapshotWrapper holdingsChildMatchWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(1)
      .withContentType(MATCH_PROFILE)
      .withReactTo(NON_MATCH)
      .withContent(JsonObject.mapFrom(new MatchProfile().withName("holdingsChildMatchWrapper").withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(HOLDINGS)))
      .withChildSnapshotWrappers(List.of(holdingsUpdateActionWrapper, holdingsCreateActionWrapper));

    ProfileSnapshotWrapper holdingsParentMatchWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(1)
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(new MatchProfile().withName("holdingsParentMatchWrapper").withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(HOLDINGS)))
      .withChildSnapshotWrappers(List.of(holdingsChildMatchWrapper, holdingsUpdateActionWrapper));

    ProfileSnapshotWrapper jobProfileWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile().withName("jobProfileWrapper")))
      .withChildSnapshotWrappers(List.of(instanceParentMatchWrapper, holdingsParentMatchWrapper));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_UPDATED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(instanceUpdateActionWrapper2);

    // when
    EventManager.handleEvent(eventPayload, jobProfileWrapper).whenComplete((eventContext, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertEquals(holdingsParentMatchWrapper.getId(), eventContext.getCurrentNode().getId());
      testContext.assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), eventContext.getEventType());
      async.complete();
    });
  }

  //
  @Test
  public void shouldHandleAndSetToCurrentNodeMatchWrapper2(TestContext testContext) {
    Async async = testContext.async();
    // given
    EventHandler updateInstanceHandler = Mockito.mock(EventHandler.class);
    Mockito.doAnswer(invocationOnMock -> {
      DataImportEventPayload payload = invocationOnMock.getArgument(0);
      payload.setCurrentNode(payload.getCurrentNode().getChildSnapshotWrappers().get(0));
      return CompletableFuture.completedFuture(payload.withEventType(DI_INVENTORY_INSTANCE_UPDATED.value()));
    }).when(updateInstanceHandler).handle(any(DataImportEventPayload.class));
    Mockito.when(updateInstanceHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);

    EventManager.registerEventHandler(updateInstanceHandler);

    ProfileSnapshotWrapper mappingWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(0)
      .withContentType(MAPPING_PROFILE)
      .withContent(JsonObject.mapFrom(new MappingProfile().withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(INSTANCE)));

    ProfileSnapshotWrapper actionWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withReactTo(MATCH)
      .withOrder(0)
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.INSTANCE)))
      .withChildSnapshotWrappers(Collections.singletonList(mappingWrapper));

    ProfileSnapshotWrapper matchWrapper1 = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(0)
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(new MatchProfile().withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(INSTANCE)));

    ProfileSnapshotWrapper matchWrapper2 = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(1)
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(new MatchProfile().withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(HOLDINGS)));

    ProfileSnapshotWrapper jobProfileWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()))
      .withChildSnapshotWrappers(Arrays.asList(
        matchWrapper1.withChildSnapshotWrappers(Collections.singletonList(actionWrapper)), matchWrapper2));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(actionWrapper);

    // when
    EventManager.handleEvent(eventPayload, jobProfileWrapper).whenComplete((eventContext, throwable) -> {
    // then
      testContext.assertNull(throwable);
      testContext.assertEquals(matchWrapper2.getId(), eventContext.getCurrentNode().getId());
      testContext.assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), eventContext.getEventType());
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

    ProfileSnapshotWrapper jobProfileSnapshot = new ProfileSnapshotWrapper()
      .withId(jobProfileId)
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()))
      .withChildSnapshotWrappers(Collections.singletonList(
        new ProfileSnapshotWrapper()
          .withId(actionProfileId)
          .withContentType(ACTION_PROFILE)
          .withContent(JsonObject.mapFrom(new ActionProfile().withAction(UPDATE).withFolioRecord(ActionProfile.FolioRecord.INSTANCE)))));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>());
    // when
    EventManager.handleEvent(eventPayload, jobProfileSnapshot).whenComplete((payload, throwable) -> {
    // then
      testContext.assertNull(throwable);
      HashMap<String, String> context = payload.getContext();
      testContext.assertEquals(UpdateInstanceEventHandler.POST_PROC_INIT_EVENT, payload.getEventType());
      testContext.assertEquals(UpdateInstanceEventHandler.POST_PROC_RESULT_EVENT, context.get(EventManager.POST_PROCESSING_RESULT_EVENT_KEY));

      testContext.assertEquals(1, payload.getEventsChain().size());
      testContext.assertEquals(1, payload.getCurrentNodePath().size());
      testContext.assertEquals(payload.getCurrentNodePath(), Collections.singletonList(jobProfileId));
      testContext.assertEquals(payload.getEventsChain(), Collections.singletonList(DI_INCOMING_MARC_BIB_RECORD_PARSED.value()));
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

    ProfileSnapshotWrapper jobProfileSnapshot = new ProfileSnapshotWrapper()
      .withId(jobProfileId)
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()))
      .withChildSnapshotWrappers(Collections.singletonList(
        new ProfileSnapshotWrapper()
          .withId(actionProfileId)
          .withContentType(ACTION_PROFILE)
          .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.INSTANCE)))));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(UpdateInstanceEventHandler.POST_PROC_INIT_EVENT)
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(payloadContext);
    // when
    EventManager.handleEvent(eventPayload, jobProfileSnapshot).whenComplete((payload, throwable) -> {
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

  @Test
  public void shouldClearExtraOLKeyFromPayload(TestContext testContext) {
    Async async = testContext.async();
    // given
    EventManager.registerEventHandler(new CreateInstanceEventHandler());
    EventManager.registerEventHandler(new CreateHoldingsRecordEventHandler());
    EventManager.registerEventHandler(new CreateItemRecordEventHandler());
    EventManager.registerEventHandler(new CreateAuthorityEventHandler());

    ProfileSnapshotWrapper jobProfileSnapshot = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()))
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM)))));

    HashMap<String, String> extraOLKey = new HashMap<>();
    extraOLKey.put("OL_ACCUMULATIVE_RESULTS", "test data");
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_HOLDINGS_RECORD_CREATED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(extraOLKey)
      .withCurrentNode(jobProfileSnapshot.getChildSnapshotWrappers().get(0));
    // when
    EventManager.handleEvent(eventPayload, jobProfileSnapshot).whenComplete((nextEventContext, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertEquals(2, nextEventContext.getEventsChain().size());
      testContext.assertEquals(
        nextEventContext.getEventsChain(),
        Arrays.asList("DI_HOLDINGS_RECORD_CREATED", "DI_ITEM_RECORD_CREATED")
      );
      testContext.assertEquals("DI_COMPLETED", nextEventContext.getEventType());
      testContext.assertNull(nextEventContext.getContext().get("OL_ACCUMULATIVE_RESULTS"));
      async.complete();
    });
  }
}
