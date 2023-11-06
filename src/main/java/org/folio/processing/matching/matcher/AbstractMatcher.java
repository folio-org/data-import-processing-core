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
import org.folio.processing.matching.reader.util.MatchIdProcessorUtil;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.folio.processing.matching.reader.util.MatchIdProcessorUtil.MAPPING_PARAMS_KEY;
import static org.folio.processing.matching.reader.util.MatchIdProcessorUtil.RELATIONS_KEY;

public class AbstractMatcher implements Matcher {
  private final MatchValueReader matchValueReader;
  private final MatchValueLoader matchValueLoader;
  protected static final String NOT_MATCHED_NUMBER = "NOT_MATCHED_NUMBER";

  public AbstractMatcher(MatchValueReader matchValueReader, MatchValueLoader matchValueLoader) {
    this.matchValueReader = matchValueReader;
    this.matchValueLoader = matchValueLoader;
  }

  @Override
  public CompletableFuture<Boolean> match(DataImportEventPayload eventPayload) {
    HashMap<String, String> payloadContext = eventPayload.getContext();
    payloadContext.remove(NOT_MATCHED_NUMBER);
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

    payloadContext.remove(RELATIONS_KEY);
    payloadContext.remove(MAPPING_PARAMS_KEY);
    return performMatching(value, matchDetail, eventPayload);
  }

  protected CompletableFuture<Boolean> performMatching(Value value, MatchDetail matchDetail, DataImportEventPayload eventPayload) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    loadEntity(value, matchDetail, eventPayload)
      .whenComplete((loadResult, throwable) -> {
        if (throwable != null) {
          future.completeExceptionally(throwable);
        } else {
          if (loadResult.getValue() != null) {
            eventPayload.getContext().put(loadResult.getEntityType(), loadResult.getValue());
            future.complete(true);
          } else {
            eventPayload.getContext().put(NOT_MATCHED_NUMBER, String.valueOf(1));
            future.complete(false);
          }
        }
      });
    return future;
  }

  protected CompletableFuture<LoadResult> loadEntity(Value value, MatchDetail matchDetail, DataImportEventPayload eventPayload) {
    LoadQuery query = LoadQueryBuilder.build(value, matchDetail);
    return matchValueLoader.loadEntity(query, eventPayload);
  }
}
