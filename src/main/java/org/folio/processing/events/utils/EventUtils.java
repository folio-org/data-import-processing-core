package org.folio.processing.events.utils;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.publisher.KafkaEventPublisher;

/**
 * Utility class for event-related operations
 */
public final class EventUtils {

  private EventUtils() {
  }

  /**
   * Safely extracts recordId from event payload context.
   * Returns empty string if recordId is null or context is null.
   *
   * @param eventPayload the event payload
   * @return recordId
   */
  public static String extractRecordId(DataImportEventPayload eventPayload) {
    if (eventPayload == null || eventPayload.getContext() == null) {
      return "";
    }
    return eventPayload.getContext().get(KafkaEventPublisher.RECORD_ID_HEADER);
  }
}
