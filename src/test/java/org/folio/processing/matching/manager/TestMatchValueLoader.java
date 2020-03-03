package org.folio.processing.matching.manager;

import org.folio.DataImportEventPayload;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;

public class TestMatchValueLoader implements MatchValueLoader {
  @Override
  public LoadResult loadEntity(LoadQuery loadQuery, DataImportEventPayload eventPayload) {
    LoadResult result = new LoadResult();
    result.setValue("Some value");
    result.setEntityType("MARC");
    return result;
  }

  @Override
  public boolean isEligibleForEntityType(EntityType existingRecordType) {
    return existingRecordType == MARC_BIBLIOGRAPHIC;
  }
}
