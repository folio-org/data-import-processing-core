package org.folio.processing.matching.reader;

import org.folio.processing.events.model.EventContext;
import org.folio.processing.matching.model.schemas.MatchDetail;
import org.folio.processing.matching.model.schemas.MatchProfile;
import org.folio.processing.value.Value;

/**
 * MatchValueReader interface
 */
public interface MatchValueReader {

  /**
   * Extracts value from the Record that matches specified MatchDetails
   *
   * @param context EventContext containing the Record in its objects field
   * @param matchDetail MatchDetail containing details, by which value should be extracted from the Record
   * @return Value from the Record that matches specified conditions
   */
  Value read(EventContext context, MatchDetail matchDetail);

  /**
   * Defines whether specific implementation of the MatchValueReader is suited for the specified IncomingRecordType
   *
   * @param incomingRecordType incoming Record type
   * @return true if MatchValueReader is suited for reading specified Record type
   */
  boolean isEligibleForEntityType(MatchProfile.IncomingRecordType incomingRecordType);
}
