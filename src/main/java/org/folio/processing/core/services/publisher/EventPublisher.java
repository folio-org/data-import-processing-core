package org.folio.processing.core.services.publisher;

import org.folio.processing.core.model.EventContext;
import org.folio.rest.jaxrs.model.Event;

import java.util.concurrent.CompletableFuture;

/**
 * Event publisher
 */
public interface EventPublisher {
  /**
   * Sends event to consumer service, which may be mod-pubsub or other ones.
   *
   * @param context event context
   * @return future with event context
   */
  CompletableFuture<Event> publish(EventContext context);
}
