package org.folio.processing.core.services.processor;

import io.vertx.core.Future;
import org.folio.processing.core.model.Context;

public interface EventProcessor {

  /**
   * @param context
   * @return
   */
  Future<Context> process(Context context);
}
