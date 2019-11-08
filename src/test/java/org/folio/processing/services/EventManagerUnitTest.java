package org.folio.processing.services;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.processing.core.EventManager;
import org.folio.processing.core.model.EventContext;
import org.folio.processing.core.util.EventContextUtil;
import org.folio.processing.services.handlers.CreateHoldingsRecordEventHandler;
import org.folio.processing.services.handlers.CreateInstanceEventHandler;
import org.folio.processing.services.handlers.CreateItemRecordEventHandler;
import org.folio.processing.services.handlers.FailExceptionallyHandler;
import org.folio.processing.services.handlers.ThrowExceptionHandler;
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
    EventContext eventContext = new EventContext("CREATED_SRS_MARC_BIB_RECORD", okapiConnectionParams);
    String eventPayload = EventContextUtil.toEventPayload(eventContext);
    // when
    EventManager.handleEvent(eventPayload).whenComplete((nextEventContext, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertTrue(nextEventContext.isHandled());
      testContext.assertEquals(expectedEventChainSize, nextEventContext.getEventChain().size());
      testContext.assertEquals(
        nextEventContext.getEventChain(),
        Arrays.asList("CREATED_SRS_MARC_BIB_RECORD", "CREATED_INVENTORY_INSTANCE", "CREATED_HOLDINGS_RECORD")
      );
      testContext.assertEquals("CREATED_ITEM_RECORD", nextEventContext.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldNotHandleEventIfNoHandlersDefined(TestContext testContext) {
    Async async = testContext.async();
    // given
    int expectedEventChainSize = 0;
    EventContext eventContext = new EventContext("CREATED_SRS_MARC_BIB_RECORD", okapiConnectionParams);
    String eventPayload = EventContextUtil.toEventPayload(eventContext);
    // when
    EventManager.handleEvent(eventPayload).whenComplete((p, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertFalse(eventContext.isHandled());
      testContext.assertEquals(expectedEventChainSize, eventContext.getEventChain().size());
      testContext.assertEquals("CREATED_SRS_MARC_BIB_RECORD", eventContext.getEventType());
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
    EventContext eventContext = new EventContext("UNDEFINED_EVENT", okapiConnectionParams);
    String eventPayload = EventContextUtil.toEventPayload(eventContext);
    // when
    EventManager.handleEvent(eventPayload).whenComplete((p, throwable) -> {
      // then
      testContext.assertNull(throwable);
      testContext.assertFalse(eventContext.isHandled());
      testContext.assertEquals(expectedEventChainSize, eventContext.getEventChain().size());
      testContext.assertEquals("UNDEFINED_EVENT", eventContext.getEventType());
      async.complete();
    });
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionOnHandlerThrowException(TestContext testContext) {
    Async async = testContext.async();
    // given
    EventManager.registerEventHandler(new ThrowExceptionHandler());
    EventContext eventContext = new EventContext("CREATED_SRS_MARC_BIB_RECORD", okapiConnectionParams);
    String eventPayload = EventContextUtil.toEventPayload(eventContext);
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
    EventContext eventContext = new EventContext("CREATED_SRS_MARC_BIB_RECORD", okapiConnectionParams);
    String eventPayload = EventContextUtil.toEventPayload(eventContext);
    // when
    EventManager.handleEvent(eventPayload).whenComplete((nextEventContext, throwable) -> {
      // then
      testContext.assertNotNull(throwable);
      testContext.assertEquals("java.lang.IllegalArgumentException: Can not handle event context", throwable.getMessage());
      testContext.assertFalse(eventContext.isHandled());
      testContext.assertEquals(expectedEventChainSize, eventContext.getEventChain().size());
      testContext.assertEquals("CREATED_SRS_MARC_BIB_RECORD", eventContext.getEventType());
      async.complete();
    });
  }
}
