package org.folio.processing.matching.matcher;

import io.vertx.core.json.JsonObject;

import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.loader.query.LoadQueryBuilder;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.matching.reader.util.MatchIdProcessorUtil;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.folio.processing.matching.reader.util.MatchIdProcessorUtil.MAPPING_PARAMS_KEY;
import static org.folio.processing.matching.reader.util.MatchIdProcessorUtil.RELATIONS_KEY;

public interface Matcher {

  default CompletableFuture<Boolean> match(MatchValueReader matchValueReader, MatchValueLoader matchValueLoader, DataImportEventPayload eventPayload) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
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
    if (value != null && value.getType().equals(Value.ValueType.STRING)) {
      value = MatchIdProcessorUtil.retrieveIdFromContext(matchDetail.getExistingMatchExpression().getFields().get(0).getValue(),
        eventPayload, (StringValue) value);
    }

    eventPayload.getContext().remove(RELATIONS_KEY);
    eventPayload.getContext().remove(MAPPING_PARAMS_KEY);
    LoadQuery query = LoadQueryBuilder.build(value, matchDetail);
    matchValueLoader.loadEntity(query, eventPayload)
      .whenComplete((loadResult, throwable) -> {
        if (throwable != null) {
          future.completeExceptionally(throwable);
        } else {
          if (loadResult.getValue() != null) {
            eventPayload.getContext().put(loadResult.getEntityType(), loadResult.getValue());
            future.complete(true);
          } else {
            future.complete(false);
          }
        }
      });
    return future;
  }
}
