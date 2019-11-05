package org.folio.processing.core.util;

import io.vertx.core.json.JsonObject;
import org.folio.processing.core.model.EventContext;

public final class EventContextUtil {
  private static final String PAYLOAD_CONTEXT_KEY = "context";

  private EventContextUtil() {
  }

  /**
   * Extracts EventContext from payload
   *
   * @param eventPayload given payload of event represented as string
   * @return event context
   */
  public static EventContext fromEventPayload(String eventPayload) {
    EventContext eventContext = new JsonObject(eventPayload).getJsonObject(PAYLOAD_CONTEXT_KEY).mapTo(EventContext.class);
    return eventContext;
  }

  /**
   * Creates new payload with given event context
   *
   * @param eventContext event context
   * @return event payload
   */
  public static String toEventPayload(EventContext eventContext) {
    String eventPayload = new JsonObject().put(PAYLOAD_CONTEXT_KEY, JsonObject.mapFrom(eventContext)).encode();
    return eventPayload;
  }
}
