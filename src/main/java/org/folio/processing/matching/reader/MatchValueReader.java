package org.folio.processing.matching.reader;

import org.folio.processing.events.model.EventContext;
import org.folio.processing.matching.model.schemas.MatchDetail;
import org.folio.processing.matching.model.schemas.MatchProfile;
import org.folio.processing.value.Value;

public interface MatchValueReader {

  Value read(EventContext context, MatchDetail matchDetail);

  boolean isEligibleForEntityType(MatchProfile.IncomingRecordType incomingRecordType);
}
