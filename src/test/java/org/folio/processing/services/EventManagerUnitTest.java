package org.folio.processing.services;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.processing.core.EventManager;
import org.folio.processing.core.model.EventContext;
import org.folio.processing.core.services.handler.EventHandler;
import org.folio.processing.services.handlers.CreateHoldingsRecordEventHandler;
import org.folio.processing.services.handlers.CreateInstanceEventHandler;
import org.folio.processing.services.handlers.CreateItemRecordEventHandler;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

@RunWith(VertxUnitRunner.class)
public class EventManagerUnitTest {

  private EventHandler createInstanceEventHandler = new CreateInstanceEventHandler();
  private EventHandler createHoldingsRecordEventHandler = new CreateHoldingsRecordEventHandler();
  private EventHandler createItemRecordEventHandler = new CreateItemRecordEventHandler();

  @Test
  public void shouldHandleEvent(TestContext testContext) {
    // given
    EventManager eventManager = new EventManager();
    eventManager.registerHandler(createInstanceEventHandler);
    eventManager.registerHandler(createHoldingsRecordEventHandler);
    eventManager.registerHandler(createItemRecordEventHandler);

    int expectedEventChainSize = 3;

    EventContext eventContext = new EventContext();
    eventContext.setEventType("CREATED_SRS_MARC_BIB_RECORD");

    // when
    eventManager.handleEvent(eventContext).setHandler(ar -> {
      // then
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(eventContext.isHandled());
      testContext.assertEquals(expectedEventChainSize, eventContext.getEventChain().size());
      testContext.assertEquals(
        eventContext.getEventChain(),
        Arrays.asList("CREATED_SRS_MARC_BIB_RECORD", "CREATED_INVENTORY_INSTANCE", "CREATED_HOLDINGS_RECORD")
      );
      testContext.assertEquals("CREATED_ITEM_RECORD", eventContext.getEventType());
    });
  }

  @Test
  public void shouldNotHandleEventIfNoHandlersDefined(TestContext testContext) {
    // given
    EventManager eventManager = new EventManager();
    int expectedEventChainSize = 0;

    EventContext eventContext = new EventContext();
    eventContext.setEventType("CREATED_SRS_MARC_BIB_RECORD");

    // when
    eventManager.handleEvent(eventContext).setHandler(ar -> {
      // then
      testContext.assertTrue(ar.succeeded());
      testContext.assertFalse(eventContext.isHandled());
      testContext.assertEquals(expectedEventChainSize, eventContext.getEventChain().size());
      testContext.assertEquals("CREATED_SRS_MARC_BIB_RECORD", eventContext.getEventType());
    });
  }

  @Test
  public void shouldNotHandleEventIfNoHandlersFound(TestContext testContext) {
    EventManager eventManager = new EventManager();
    eventManager.registerHandler(createInstanceEventHandler);
    eventManager.registerHandler(createHoldingsRecordEventHandler);
    eventManager.registerHandler(createItemRecordEventHandler);

    int expectedEventChainSize = 0;

    EventContext eventContext = new EventContext();
    eventContext.setEventType("UNDEFINED_EVENT");

    // when
    eventManager.handleEvent(eventContext).setHandler(ar -> {
      // then
      testContext.assertTrue(ar.succeeded());
      testContext.assertFalse(eventContext.isHandled());
      testContext.assertEquals(expectedEventChainSize, eventContext.getEventChain().size());
      testContext.assertEquals("UNDEFINED_EVENT", eventContext.getEventType());
    });
  }
}
