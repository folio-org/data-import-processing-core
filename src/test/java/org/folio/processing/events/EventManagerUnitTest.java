package org.folio.processing.events;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.handlers.CreateHoldingsRecordEventHandler;
import org.folio.processing.events.handlers.CreateInstanceEventHandler;
import org.folio.processing.events.handlers.CreateItemRecordEventHandler;
import org.folio.processing.events.handlers.FailExceptionallyHandler;
import org.folio.processing.events.handlers.ThrowExceptionHandler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

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
    int expectedEventChainSize = 3;
    EventManager.registerEventHandler(new CreateInstanceEventHandler());
    EventManager.registerEventHandler(new CreateHoldingsRecordEventHandler());
    EventManager.registerEventHandler(new CreateItemRecordEventHandler());
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_SRS_MARC_BIB_RECORD_CREATED")
      .withTenant(okapiConnectionParams.getTenantId())
      .withOkapiUrl(okapiConnectionParams.getOkapiUrl())
      .withToken(okapiConnectionParams.getToken());
    // when
    EventManager.handleEvent(eventPayload).whenComplete((nextEventContext, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertEquals(expectedEventChainSize, nextEventContext.getEventsChain().size());
      testContext.assertEquals(
        nextEventContext.getEventsChain(),
        Arrays.asList("DI_SRS_MARC_BIB_RECORD_CREATED", "DI_INVENTORY_INSTANCE_CREATED", "DI_HOLDINGS_RECORD_CREATED")
      );
      testContext.assertEquals("DI_ITEM_RECORD_CREATED", nextEventContext.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldNotHandleEventIfNoHandlersDefined(TestContext testContext) {
    Async async = testContext.async();
    // given
    int expectedEventChainSize = 0;
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_SRS_MARC_BIB_RECORD_CREATED")
      .withTenant(okapiConnectionParams.getTenantId())
      .withOkapiUrl(okapiConnectionParams.getOkapiUrl())
      .withToken(okapiConnectionParams.getToken());
    EventManager.handleEvent(eventPayload).whenComplete((p, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertEquals(expectedEventChainSize, eventPayload.getEventsChain().size());
      testContext.assertEquals("DI_SRS_MARC_BIB_RECORD_CREATED", eventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldNotHandleEventIfNoHandlersFound(TestContext testContext) {
    Async async = testContext.async();
    // given
    int expectedEventChainSize = 0;
    EventManager.registerEventHandler(new CreateInstanceEventHandler());
    EventManager.registerEventHandler(new CreateHoldingsRecordEventHandler());
    EventManager.registerEventHandler(new CreateItemRecordEventHandler());
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("UNDEFINED_EVENT")
      .withTenant(okapiConnectionParams.getTenantId())
      .withOkapiUrl(okapiConnectionParams.getOkapiUrl())
      .withToken(okapiConnectionParams.getToken());
    // when
    EventManager.handleEvent(eventPayload).whenComplete((p, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertEquals(expectedEventChainSize, eventPayload.getEventsChain().size());
      testContext.assertEquals("UNDEFINED_EVENT", eventPayload.getEventType());
      async.complete();
    });
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionOnHandlerThrowException(TestContext testContext) {
    Async async = testContext.async();
    // given
    EventManager.registerEventHandler(new ThrowExceptionHandler());
    DataImportEventPayload eventPayload = new DataImportEventPayload().withEventType("DI_SRS_MARC_BIB_RECORD_CREATED");
    // when
    EventManager.handleEvent(eventPayload).whenComplete((p, throwable) -> {
      async.complete();
    });
  }

  @Test
  public void shouldReturnExceptionallyCompletedFutureOnHandlerFail(TestContext testContext) {
    Async async = testContext.async();
    // given
    int expectedEventChainSize = 0;
    EventManager.registerEventHandler(new FailExceptionallyHandler());
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_SRS_MARC_BIB_RECORD_CREATED")
      .withTenant(okapiConnectionParams.getTenantId())
      .withOkapiUrl(okapiConnectionParams.getOkapiUrl())
      .withToken(okapiConnectionParams.getToken());
    // when
    EventManager.handleEvent(eventPayload).whenComplete((nextEventContext, throwable) -> {
      // then
      testContext.assertNotNull(throwable);
      testContext.assertEquals("java.lang.IllegalArgumentException: Can not handle event payload", throwable.getMessage());
      testContext.assertEquals(expectedEventChainSize, eventPayload.getEventsChain().size());
      testContext.assertEquals("DI_SRS_MARC_BIB_RECORD_CREATED", eventPayload.getEventType());
      async.complete();
    });
  }
}
