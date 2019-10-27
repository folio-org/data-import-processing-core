package org.folio.processing.services;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.processing.core.model.Context;
import org.folio.processing.core.services.EventManager;
import org.folio.processing.core.services.EventManagerImpl;
import org.folio.processing.core.services.handler.EventHandler;
import org.folio.processing.core.services.processor.EventProcessor;
import org.folio.processing.core.services.processor.RecursiveEventProcessor;
import org.junit.Test;
import org.junit.internal.runners.JUnit38ClassRunner;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

@RunWith(VertxUnitRunner.class)
public class EventManagerUnitTest {
  private List<EventHandler> eventHandlers = new ArrayList<>();
  private EventProcessor eventProcessor = new RecursiveEventProcessor(eventHandlers);
  private EventManager eventManager = new EventManagerImpl(eventProcessor);

  @Test
  public void shouldHandleEvent() {
  }

  @Test
  public void shouldNotHandleEventIfNoHandlersDefined() {
  }

  @Test
  public void shouldNotHandleEventIfNoHandlersFound() {
  }
}
