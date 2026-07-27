package org.folio.processing.mapping.defaultmapper.processor;

import io.vertx.core.json.JsonObject;

public final class ProcessorHelper {

  private ProcessorHelper() { }

  public static String[] getFunctionsFromCondition(JsonObject condition) {
    return condition.getString("type").split(",");
  }
}
