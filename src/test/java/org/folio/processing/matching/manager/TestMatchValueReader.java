package org.folio.processing.matching.manager;

import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;

public class TestMatchValueReader implements MatchValueReader {
    @Override
    public Value read(DataImportEventPayload eventPayload, MatchDetail matchDetail) {
      return null;
    }

    @Override
    public boolean isEligibleForEntityType(EntityType incomingRecordType) {
      return incomingRecordType == EDIFACT_INVOICE;
    }
}
