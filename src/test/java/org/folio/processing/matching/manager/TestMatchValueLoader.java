package org.folio.processing.matching.manager;

import org.folio.processing.events.model.EventContext;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.model.schemas.MatchProfile;

import static org.folio.processing.matching.model.schemas.MatchProfile.ExistingRecordType.MARC_BIBLIOGRAPHIC;

public class TestMatchValueLoader implements MatchValueLoader {
  @Override
  public LoadResult loadEntity(LoadQuery loadQuery, EventContext eventContext) {
    LoadResult result = new LoadResult();
    result.setValue("Some value");
    return result;
  }

  @Override
  public boolean isEligibleForEntityType(MatchProfile.ExistingRecordType existingRecordType) {
    return existingRecordType == MARC_BIBLIOGRAPHIC;
  }
}
