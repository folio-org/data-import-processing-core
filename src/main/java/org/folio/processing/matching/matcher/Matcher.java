package org.folio.processing.matching.matcher;

import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.loader.query.LoadQueryBuilder;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.Map;

public interface Matcher {

  default boolean match(MatchValueReader matchValueReader, MatchValueLoader matchValueLoader, DataImportEventPayload eventPayload) {
    ProfileSnapshotWrapper matchingProfileWrapper = eventPayload.getCurrentNode();
    MatchProfile matchProfile;
    if (matchingProfileWrapper.getContent() instanceof Map) {
      matchProfile = new JsonObject((Map) matchingProfileWrapper.getContent()).mapTo(MatchProfile.class);
    } else {
      matchProfile = (MatchProfile) matchingProfileWrapper.getContent();
    }
    // Only one matching detail is expected in first implementation,
    // in future matching will support multiple matching details combined in logic expressions
    MatchDetail matchDetail = matchProfile.getMatchDetails().get(0);

    Value value = matchValueReader.read(eventPayload, matchDetail);
    LoadQuery query = LoadQueryBuilder.build(value, matchDetail);
    LoadResult result = matchValueLoader.loadEntity(query, eventPayload);
    if (result.getValue() != null) {
      eventPayload.getContext().put(result.getEntityType(), result.getValue());
      return true;
    }
    return false;
  }
}
