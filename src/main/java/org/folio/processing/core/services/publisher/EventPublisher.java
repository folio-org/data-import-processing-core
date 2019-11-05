package org.folio.processing.core.services.publisher;

import io.vertx.core.Future;
import org.folio.processing.core.model.EventContext;

/**
 * Event publisher
 */
public interface EventPublisher {
  /**
   * Sends event to consumer service, which may be mod-pubsub or other ones.
   *
   * @param context even context
   * @return future
   */
  Future<Void> publish(EventContext context);
}
