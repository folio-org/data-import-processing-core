package org.folio.processing.events;

import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.ReactToType.MATCH;
import static org.folio.rest.jaxrs.model.ReactToType.NON_MATCH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.folio.processing.events.services.publisher.EventPublisher;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.ReactToType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

@ExtendWith(VertxExtension.class)
class EventManagerUnitTest {
  private static final Logger LOGGER = LogManager.getLogger(EventManagerUnitTest.class);

  private static final String TOKEN = "token";
  private static final String TENANT_ID = "diku";
  private static final String CONNECTION_URL = "http://localhost:9000";

  @BeforeEach
  void beforeTest() {
    EventManager.clearEventHandlers();
    var eventPublisher = mock(EventPublisher.class);
    EventManager.registerCustomKafkaEventPublisher(eventPublisher);
    when(eventPublisher.publish(any())).thenReturn(CompletableFuture.completedFuture(new Event()));
  }

  @Test
  void shouldHandleEvent(VertxTestContext testContext) {
    LOGGER.info("test:: shouldHandleEvent");
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
                  .withContent(
                    JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM)))))))));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INCOMING_MARC_BIB_RECORD_PARSED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(CONNECTION_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshot.getChildSnapshotWrappers().getFirst());
    // when
    EventManager.handleEvent(eventPayload, profileSnapshot)
      .whenComplete((nextEventContext, throwable) -> {
        testContext.verify(() -> {
          // then
          assertNull(throwable);
          assertEquals(1, nextEventContext.getEventsChain().size());
          assertEquals(
            nextEventContext.getEventsChain(),
            Collections.singletonList("DI_INCOMING_MARC_BIB_RECORD_PARSED")
          );
          assertEquals("DI_INVENTORY_INSTANCE_CREATED", nextEventContext.getEventType());
        });
        testContext.completeNow();
      });
  }

  @Test
  void shouldHandleLastEvent(VertxTestContext testContext) {
    LOGGER.info("test:: shouldHandleLastEvent");
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
      .withOkapiUrl(CONNECTION_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(jobProfileSnapshot.getChildSnapshotWrappers().getFirst());
    // when
    EventManager.handleEvent(eventPayload, jobProfileSnapshot).whenComplete((nextEventContext, throwable) -> {
      testContext.verify(() -> {
        // then
        assertNull(throwable);
        assertEquals(2, nextEventContext.getEventsChain().size());
        assertEquals(
          nextEventContext.getEventsChain(),
          Arrays.asList("DI_HOLDINGS_RECORD_CREATED", "DI_ITEM_RECORD_CREATED")
        );
        assertEquals("DI_COMPLETED", nextEventContext.getEventType());
      });
      testContext.completeNow();
    });
  }

  @Test
  void shouldIgnoreEventIfNoHandlersDefined(VertxTestContext testContext) {
    LOGGER.info("test:: shouldIgnoreEventIfNoHandlersDefined");
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
      .withOkapiUrl(CONNECTION_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshot.getChildSnapshotWrappers().getFirst());

    // when
    EventManager.handleEvent(eventPayload, profileSnapshot).whenComplete((nextEventContext, throwable) -> {
      testContext.verify(() -> {
        // then
        assertNull(throwable);
        assertEquals(0, eventPayload.getEventsChain().size());
        assertEquals("DI_HOLDINGS_RECORD_CREATED", eventPayload.getEventType());
      });
      testContext.completeNow();
    });
  }

  @Test
  void shouldHandleAsErrorEventIfHandlerCompletedExceptionally(VertxTestContext testContext) {
    LOGGER.info("test:: shouldHandleAsErrorEventIfHandlerCompletedExceptionally");
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
      .withOkapiUrl(CONNECTION_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(jobProfileSnapshot.getChildSnapshotWrappers().getFirst());
    // when
    EventManager.handleEvent(eventPayload, jobProfileSnapshot).whenComplete((nextEventContext, throwable) -> {
      testContext.verify(() -> {
        // then
        assertNull(throwable);
        assertEquals(1, eventPayload.getEventsChain().size());
        assertEquals("DI_ERROR", eventPayload.getEventType());
      });
      testContext.completeNow();
    });
  }

  @Test
  void shouldHandleFirstEventInJobProfile(VertxTestContext testContext) {
    LOGGER.info("test:: shouldHandleFirstEventInJobProfile");
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
      .withOkapiUrl(CONNECTION_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>());
    // when
    EventManager.handleEvent(eventPayload, jobProfileSnapshot).whenComplete((eventContext, throwable) -> {
      testContext.verify(() -> {
        // then
        assertNull(throwable);
        assertEquals(2, eventContext.getEventsChain().size());
        assertEquals(2, eventContext.getCurrentNodePath().size());
        assertEquals(
          eventContext.getCurrentNodePath(),
          Arrays.asList(jobProfileId, actionProfileId)
        );
        assertEquals(
          eventContext.getEventsChain(),
          Arrays.asList("DI_INCOMING_MARC_BIB_RECORD_PARSED", "DI_INVENTORY_INSTANCE_CREATED")
        );
        assertEquals("DI_COMPLETED", eventContext.getEventType());
      });
      testContext.completeNow();
    });
  }

  @Test
  void shouldHandleAndSetToCurrentNodeAction2Wrapper(VertxTestContext testContext) {
    LOGGER.info("test:: shouldHandleAndSetToCurrentNodeAction2Wrapper");
    // given
    CreateInstanceEventHandler createInstanceHandler = Mockito.spy(new CreateInstanceEventHandler());
    Mockito.doAnswer(invocationOnMock -> {
      DataImportEventPayload payload = invocationOnMock.getArgument(0);
      payload.setCurrentNode(payload.getCurrentNode().getChildSnapshotWrappers().getFirst());
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
          .withContent(JsonObject.mapFrom(
            new MatchProfile().withIncomingRecordType(INSTANCE).withExistingRecordType(MARC_BIBLIOGRAPHIC)))
          .withChildSnapshotWrappers(Arrays.asList(action1Wrapper, action2Wrapper))));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_NOT_MATCHED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(CONNECTION_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(action1Wrapper);

    // when
    EventManager.handleEvent(eventPayload, jobProfileWrapper).whenComplete((eventContext, throwable) -> {
      testContext.verify(() -> {
        // then
        assertNull(throwable);
        assertEquals(action2Wrapper.getId(), eventContext.getCurrentNode().getId());
        assertEquals(1, eventContext.getEventsChain().size());
        assertEquals(
          Collections.singletonList(DI_INVENTORY_INSTANCE_NOT_MATCHED.value()),
          eventContext.getEventsChain()
        );
        assertEquals(DI_INVENTORY_INSTANCE_CREATED.value(), eventContext.getEventType());
      });
      testContext.completeNow();
    });
  }

  @Test
  void shouldHandleAndSetToCurrentNodeAction1Wrapper(VertxTestContext testContext) {
    LOGGER.info("test:: shouldHandleAndSetToCurrentNodeAction1Wrapper");
    // given
    EventHandler matchInstanceHandler = mock(EventHandler.class);
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
      .withContent(JsonObject.mapFrom(
        new MatchProfile().withIncomingRecordType(INSTANCE).withExistingRecordType(MARC_BIBLIOGRAPHIC)));

    ProfileSnapshotWrapper jobProfileWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()))
      .withChildSnapshotWrappers(Collections.singletonList(
        matchWrapper.withChildSnapshotWrappers(Arrays.asList(action1Wrapper, action2Wrapper))));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(CONNECTION_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(matchWrapper);

    // when
    EventManager.handleEvent(eventPayload, jobProfileWrapper).whenComplete((eventContext, throwable) -> {
      testContext.verify(() -> {
        // then
        assertNull(throwable);
        assertEquals(action1Wrapper.getId(), eventContext.getCurrentNode().getId());
        assertEquals(DI_INVENTORY_INSTANCE_NOT_MATCHED.value(), eventContext.getEventType());
      });
      testContext.completeNow();
    });
  }

  @Test
  void shouldHandleEventInCascadingProfilesAndSwitchNode(VertxTestContext testContext) {
    LOGGER.info("test:: shouldHandleEventInCascadingProfilesAndSwitchNode");
    // given
    EventHandler updateInstanceHandler = mock(EventHandler.class);
    Mockito.doAnswer(invocationOnMock -> {

      DataImportEventPayload payload = invocationOnMock.getArgument(0);
      payload.setCurrentNode(payload.getCurrentNode().getChildSnapshotWrappers().getFirst());
      return CompletableFuture.completedFuture(payload.withEventType(DI_INVENTORY_INSTANCE_UPDATED.value()));
    }).when(updateInstanceHandler).handle(any(DataImportEventPayload.class));
    Mockito.when(updateInstanceHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);

    EventManager.registerEventHandler(updateInstanceHandler);

    ProfileSnapshotWrapper instanceUpdateMappingWrapper =
      mappingWrapper("instanceUpdateMappingWrapper", 0, INSTANCE);
    ProfileSnapshotWrapper instanceUpdateActionWrapper = actionWrapper(
      "instanceUpdateActionWrapper", MATCH, 0, ActionProfile.FolioRecord.INSTANCE, UPDATE,
      instanceUpdateMappingWrapper);
    ProfileSnapshotWrapper instanceUpdateActionWrapper2 = actionWrapper(
      "instanceUpdateActionWrapper2", MATCH, 0, ActionProfile.FolioRecord.INSTANCE, UPDATE,
      instanceUpdateMappingWrapper);
    ProfileSnapshotWrapper instanceCreateActionWrapper = actionWrapper(
      "instanceCreateActionWrapper", NON_MATCH, 0, ActionProfile.FolioRecord.INSTANCE, CREATE,
      mappingWrapper("instanceCreateMappingWrapper", 0, INSTANCE));
    ProfileSnapshotWrapper instanceChildMatchWrapper = matchWrapper(
      "instanceChildMatchWrapper", NON_MATCH, 0, HOLDINGS,
      instanceUpdateActionWrapper2, instanceCreateActionWrapper);
    ProfileSnapshotWrapper instanceParentMatchWrapper = matchWrapper(
      "instanceParentMatchWrapper", null, 0, INSTANCE,
      instanceChildMatchWrapper, instanceUpdateActionWrapper);

    ProfileSnapshotWrapper holdingsUpdateActionWrapper = actionWrapper(
      "holdingsUpdateActionWrapper", MATCH, 0, ActionProfile.FolioRecord.HOLDINGS, UPDATE,
      mappingWrapper("holdingsUpdateMappingWrapper", 0, HOLDINGS));
    ProfileSnapshotWrapper holdingsCreateActionWrapper = actionWrapper(
      "holdingsCreateActionWrapper", NON_MATCH, 0, ActionProfile.FolioRecord.HOLDINGS, CREATE,
      mappingWrapper("holdingsCreateMappingWrapper", 0, HOLDINGS));
    ProfileSnapshotWrapper holdingsChildMatchWrapper = matchWrapper(
      "holdingsChildMatchWrapper", NON_MATCH, 1, HOLDINGS,
      holdingsUpdateActionWrapper, holdingsCreateActionWrapper);
    ProfileSnapshotWrapper holdingsParentMatchWrapper = matchWrapper(
      "holdingsParentMatchWrapper", null, 1, HOLDINGS,
      holdingsChildMatchWrapper, holdingsUpdateActionWrapper);

    ProfileSnapshotWrapper jobProfileWrapper =
      jobProfileWrapper("jobProfileWrapper", instanceParentMatchWrapper, holdingsParentMatchWrapper);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_UPDATED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(CONNECTION_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(instanceUpdateActionWrapper2);

    // when
    EventManager.handleEvent(eventPayload, jobProfileWrapper).whenComplete((eventContext, throwable) -> {
      testContext.verify(() -> {
        // then
        assertNull(throwable);
        assertEquals(holdingsParentMatchWrapper.getId(), eventContext.getCurrentNode().getId());
        assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), eventContext.getEventType());
      });
      testContext.completeNow();
    });
  }

  @Test
  void shouldHandleAndSetToCurrentNodeMatchWrapper2(VertxTestContext testContext) {
    LOGGER.info("test:: shouldHandleAndSetToCurrentNodeMatchWrapper2");
    // given
    EventHandler updateInstanceHandler = mock(EventHandler.class);
    Mockito.doAnswer(invocationOnMock -> {
      DataImportEventPayload payload = invocationOnMock.getArgument(0);
      payload.setCurrentNode(payload.getCurrentNode().getChildSnapshotWrappers().getFirst());
      return CompletableFuture.completedFuture(payload.withEventType(DI_INVENTORY_INSTANCE_UPDATED.value()));
    }).when(updateInstanceHandler).handle(any(DataImportEventPayload.class));
    Mockito.when(updateInstanceHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);

    EventManager.registerEventHandler(updateInstanceHandler);

    ProfileSnapshotWrapper mappingWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(0)
      .withContentType(MAPPING_PROFILE)
      .withContent(JsonObject.mapFrom(
        new MappingProfile().withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(INSTANCE)));

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
      .withContent(JsonObject.mapFrom(
        new MatchProfile().withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(INSTANCE)));

    ProfileSnapshotWrapper matchWrapper2 = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(1)
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(
        new MatchProfile().withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(HOLDINGS)));

    ProfileSnapshotWrapper jobProfileWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()))
      .withChildSnapshotWrappers(Arrays.asList(
        matchWrapper1.withChildSnapshotWrappers(Collections.singletonList(actionWrapper)), matchWrapper2));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(CONNECTION_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>())
      .withCurrentNode(actionWrapper);

    // when
    EventManager.handleEvent(eventPayload, jobProfileWrapper).whenComplete((eventContext, throwable) -> {
      testContext.verify(() -> {
        // then
        assertNull(throwable);
        assertEquals(matchWrapper2.getId(), eventContext.getCurrentNode().getId());
        assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), eventContext.getEventType());
      });
      testContext.completeNow();
    });
  }

  @Test
  void shouldHandleEventAndPreparePayloadForPostProcessing(VertxTestContext testContext) {
    LOGGER.info("test:: shouldHandleEventAndPreparePayloadForPostProcessing");
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
          .withContent(JsonObject.mapFrom(
            new ActionProfile().withAction(UPDATE).withFolioRecord(ActionProfile.FolioRecord.INSTANCE)))));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(CONNECTION_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>());
    // when
    EventManager.handleEvent(eventPayload, jobProfileSnapshot).whenComplete((payload, throwable) -> {
      testContext.verify(() -> {
        // then
        assertNull(throwable);
        HashMap<String, String> context = payload.getContext();
        assertEquals(UpdateInstanceEventHandler.POST_PROC_INIT_EVENT, payload.getEventType());
        assertEquals(UpdateInstanceEventHandler.POST_PROC_RESULT_EVENT,
          context.get(EventManager.POST_PROCESSING_RESULT_EVENT_KEY));

        assertEquals(1, payload.getEventsChain().size());
        assertEquals(1, payload.getCurrentNodePath().size());
        assertEquals(payload.getCurrentNodePath(), Collections.singletonList(jobProfileId));
        assertEquals(payload.getEventsChain(), Collections.singletonList(DI_INCOMING_MARC_BIB_RECORD_PARSED.value()));
      });
      testContext.completeNow();
    });
  }

  @Test
  void shouldPerformEventPostProcessingAndPreparePayloadAfterPostProcessing(VertxTestContext testContext) {
    LOGGER.info("test:: shouldPerformEventPostProcessingAndPreparePayloadAfterPostProcessing");
    // given
    String jobProfileId = UUID.randomUUID().toString();
    String actionProfileId = UUID.randomUUID().toString();
    EventManager.registerEventHandler(new InstancePostProcessingEventHandler());

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EventManager.POST_PROCESSING_RESULT_EVENT_KEY,
      UpdateInstanceEventHandler.POST_PROC_RESULT_EVENT);

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
      .withOkapiUrl(CONNECTION_URL)
      .withToken(TOKEN)
      .withContext(payloadContext);
    // when
    EventManager.handleEvent(eventPayload, jobProfileSnapshot).whenComplete((payload, throwable) -> {
      testContext.verify(() -> {
        // then
        assertNull(throwable);
        HashMap<String, String> context = payload.getContext();
        assertEquals(DI_COMPLETED.value(), payload.getEventType());
        assertNull(context.get(EventManager.POST_PROCESSING_RESULT_EVENT_KEY));

        assertEquals(2, payload.getEventsChain().size());
        assertEquals(2, payload.getCurrentNodePath().size());
        assertEquals(payload.getCurrentNodePath(), Arrays.asList(jobProfileId, actionProfileId));
        assertEquals(payload.getEventsChain(),
          Arrays.asList(UpdateInstanceEventHandler.POST_PROC_INIT_EVENT,
            UpdateInstanceEventHandler.POST_PROC_RESULT_EVENT));
      });
      testContext.completeNow();
    });
  }

  @Test
  void shouldClearExtraOlKeyFromPayload(VertxTestContext testContext) {
    LOGGER.info("test:: shouldClearExtraOlKeyFromPayload");
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

    HashMap<String, String> extraOlKey = new HashMap<>();
    extraOlKey.put("OL_ACCUMULATIVE_RESULTS", "test data");
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_HOLDINGS_RECORD_CREATED")
      .withTenant(TENANT_ID)
      .withOkapiUrl(CONNECTION_URL)
      .withToken(TOKEN)
      .withContext(extraOlKey)
      .withCurrentNode(jobProfileSnapshot.getChildSnapshotWrappers().getFirst());
    // when
    EventManager.handleEvent(eventPayload, jobProfileSnapshot).whenComplete((nextEventContext, throwable) -> {
      testContext.verify(() -> {
        // then
        assertNull(throwable);
        assertEquals(2, nextEventContext.getEventsChain().size());
        assertEquals(
          nextEventContext.getEventsChain(),
          Arrays.asList("DI_HOLDINGS_RECORD_CREATED", "DI_ITEM_RECORD_CREATED")
        );
        assertEquals("DI_COMPLETED", nextEventContext.getEventType());
        assertNull(nextEventContext.getContext().get("OL_ACCUMULATIVE_RESULTS"));
      });
      testContext.completeNow();
    });
  }

  private ProfileSnapshotWrapper mappingWrapper(String name, int order, EntityType existingRecordType) {
    return new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(order)
      .withContentType(MAPPING_PROFILE)
      .withContent(JsonObject.mapFrom(
        new MappingProfile().withName(name).withIncomingRecordType(MARC_BIBLIOGRAPHIC)
          .withExistingRecordType(existingRecordType)));
  }

  private ProfileSnapshotWrapper actionWrapper(
    String name,
    ReactToType reactTo,
    int order,
    ActionProfile.FolioRecord folioRecord,
    ActionProfile.Action action,
    ProfileSnapshotWrapper childWrapper
  ) {
    return new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withReactTo(reactTo)
      .withOrder(order)
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(
        new ActionProfile().withName(name).withFolioRecord(folioRecord).withAction(action)))
      .withChildSnapshotWrappers(Collections.singletonList(childWrapper));
  }

  private ProfileSnapshotWrapper matchWrapper(
    String name,
    ReactToType reactTo,
    int order,
    EntityType existingRecordType,
    ProfileSnapshotWrapper... childWrappers
  ) {
    ProfileSnapshotWrapper wrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withOrder(order)
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(
        new MatchProfile().withName(name).withIncomingRecordType(MARC_BIBLIOGRAPHIC)
          .withExistingRecordType(existingRecordType)))
      .withChildSnapshotWrappers(List.of(childWrappers));
    if (reactTo != null) {
      wrapper.withReactTo(reactTo);
    }
    return wrapper;
  }

  private ProfileSnapshotWrapper jobProfileWrapper(String name, ProfileSnapshotWrapper... childWrappers) {
    return new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile().withName(name)))
      .withChildSnapshotWrappers(List.of(childWrappers));
  }
}
