package org.folio.processing.core.services.publisher;

import io.vertx.core.Future;
import org.folio.processing.core.model.EventContext;

/**
 * Event publisher
 */
public interface EventPublisher {
  /**
   * Send an event to consumer service, that is mod-pubsub or other ones.
   *
   * @param context even context
   * @return future
   */
  Future<Void> publish(EventContext context);
}
