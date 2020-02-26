package org.folio.processing.matching.manager;

import org.folio.DataImportEventPayload;
import org.folio.processing.matching.model.schemas.MatchDetail;
import org.folio.processing.matching.model.schemas.MatchProfile;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.value.Value;

import static org.folio.processing.matching.model.schemas.MatchProfile.IncomingRecordType.EDIFACT;

public class TestMatchValueReader implements MatchValueReader {
    @Override
    public Value read(DataImportEventPayload eventPayload, MatchDetail matchDetail) {
      return null;
    }

    @Override
    public boolean isEligibleForEntityType(MatchProfile.IncomingRecordType incomingRecordType) {
      return incomingRecordType == EDIFACT;
    }
}
