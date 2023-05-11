package org.folio.processing.matching.matcher;

import io.vertx.core.json.JsonObject;

import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.loader.query.LoadQueryBuilder;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.matching.reader.util.MatchIdProcessorUtil;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.folio.processing.matching.loader.query.LoadQueryBuilder.JSON_PATH_SEPARATOR;
import static org.folio.processing.matching.reader.util.MatchIdProcessorUtil.MAPPING_PARAMS_KEY;
import static org.folio.processing.matching.reader.util.MatchIdProcessorUtil.RELATIONS_KEY;

public interface Matcher {
  String MATCHED_VALUES_NUMBER = "MATCHED_VALUES_NUMBER";
  String MATCH_FIELD = "MATCH_FIELD";
  String ONE = "1";
  String ZERO = "0";

  default CompletableFuture<Boolean> match(MatchValueReader matchValueReader, MatchValueLoader matchValueLoader, DataImportEventPayload eventPayload) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    HashMap<String, String> payloadContext = eventPayload.getContext();
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

    payloadContext.put(MATCHED_VALUES_NUMBER, getMatchedValuesNumber(value));
    payloadContext.put(MATCH_FIELD, getMatchField(matchDetail));
    payloadContext.remove(RELATIONS_KEY);
    payloadContext.remove(MAPPING_PARAMS_KEY);
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

  private String getMatchedValuesNumber(Value value) {
    if (value != null) {
      if (Value.ValueType.LIST.equals(value.getType())) {
        return String.valueOf(((ListValue) value).getValue().size());
      }
      return ONE;
    }
    return ZERO;
  }

  private String getMatchField(MatchDetail matchDetail) {
    MatchExpression matchExpression = matchDetail.getExistingMatchExpression();
    if (matchExpression != null && matchExpression.getFields() != null && matchExpression.getFields().size() > 0) {
      String fieldPath = matchExpression.getFields().get(0).getValue();
      return StringUtils.substringAfter(fieldPath, JSON_PATH_SEPARATOR);
    }
    return null;
  }
}
