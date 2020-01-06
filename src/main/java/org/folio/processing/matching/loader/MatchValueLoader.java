package org.folio.processing.matching.loader;

import org.folio.processing.events.model.EventContext;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.model.schemas.MatchProfile;

public interface MatchValueLoader {

  LoadResult loadEntity(LoadQuery loadQuery, EventContext eventContext);

  boolean isEligibleForEntityType(MatchProfile.ExistingRecordType existingRecordType);
}
