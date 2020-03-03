package org.folio.processing.matching.reader;

import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;

/**
 * MatchValueReader interface
 */
public interface MatchValueReader {

  /**
   * Extracts value from the Record that matches specified MatchDetails
   *
   * @param eventPayload event payload containing the Record in its objects field
   * @param matchDetail MatchDetail containing details, by which value should be extracted from the Record
   * @return Value from the Record that matches specified conditions
   */
  Value read(DataImportEventPayload eventPayload, MatchDetail matchDetail);

  /**
   * Defines whether specific implementation of the MatchValueReader is suited for the specified IncomingRecordType
   *
   * @param incomingRecordType incoming Record type
   * @return true if MatchValueReader is suited for reading specified Record type
   */
  boolean isEligibleForEntityType(EntityType incomingRecordType);
}
