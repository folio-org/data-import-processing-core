package org.folio.processing.matching.matcher;

import org.folio.ProfileSnapshotWrapper;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.loader.query.LoadQueryBuilder;
import org.folio.processing.matching.model.schemas.MatchDetail;
import org.folio.processing.matching.model.schemas.MatchProfile;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.value.Value;

public interface Matcher {

  default boolean match(MatchValueReader matchValueReader, MatchValueLoader matchValueLoader, EventContext context) {
    ProfileSnapshotWrapper matchingProfileWrapper = context.getCurrentNode();
    MatchProfile matchProfile = (MatchProfile) matchingProfileWrapper.getContent();
    // Only one matching detail is expected in first implementation,
    // in future matching will support multiple matching details combined in logic expressions
    MatchDetail matchDetail = matchProfile.getMatchDetails().get(0);

    Value value = matchValueReader.read(context, matchDetail);
    LoadQuery query = LoadQueryBuilder.build(value, matchDetail);
    LoadResult result = matchValueLoader.loadEntity(query, context);
    if (result.getValue() != null) {
      context.getObjects().put(result.getEntityType(), result.getValue());
      return true;
    }
    return false;
  }
}
