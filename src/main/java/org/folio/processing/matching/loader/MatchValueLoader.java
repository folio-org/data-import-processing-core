package org.folio.processing.matching.loader;

import java.util.concurrent.CompletableFuture;
import org.folio.DataImportEventPayload;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.rest.jaxrs.model.EntityType;

public interface MatchValueLoader {

  CompletableFuture<LoadResult> loadEntity(LoadQuery loadQuery, DataImportEventPayload eventPayload);

  boolean isEligibleForEntityType(EntityType existingRecordType);
}
