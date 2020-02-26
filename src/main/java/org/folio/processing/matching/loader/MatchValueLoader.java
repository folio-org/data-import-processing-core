package org.folio.processing.matching.loader;

import org.folio.DataImportEventPayload;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.model.schemas.MatchProfile;

public interface MatchValueLoader {

  LoadResult loadEntity(LoadQuery loadQuery, DataImportEventPayload eventContext);

  boolean isEligibleForEntityType(MatchProfile.ExistingRecordType existingRecordType);
}
