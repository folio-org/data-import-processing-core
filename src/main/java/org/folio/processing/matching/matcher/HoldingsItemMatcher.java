package org.folio.processing.matching.matcher;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.entities.PartialError;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class HoldingsItemMatcher extends AbstractMatcher {
  private static final String ERRORS = "ERRORS";

  public HoldingsItemMatcher(MatchValueReader matchValueReader, MatchValueLoader matchValueLoader) {
    super(matchValueReader, matchValueLoader);
  }

  @Override
  public CompletableFuture<Boolean> performMatching(Value value, MatchDetail matchDetail, DataImportEventPayload eventPayload) {
    if (value instanceof ListValue) {
      return processMultipleMatching(value, matchDetail, eventPayload);
    }
    return super.performMatching(value, matchDetail, eventPayload);
  }

  private CompletableFuture<Boolean> processMultipleMatching(Value genericValue, MatchDetail matchDetail,
                                                             DataImportEventPayload eventPayload) {
    CompletableFuture<Boolean> resultFuture = new CompletableFuture<>();
    List<Value> values = ((ListValue) genericValue).getValue().stream().distinct().map(StringValue::of).collect(Collectors.toList());
    JsonArray matchedEntities = new JsonArray();
    JsonArray errors = new JsonArray();

    List<Future> multipleFutures = new ArrayList<>();
    values.forEach(v -> {
      Promise<Void> promise = Promise.promise();
      multipleFutures.add(promise.future());
      loadEntity(v, matchDetail, eventPayload)
        .whenComplete((loadResult, throwable) -> {
          if (throwable != null) {
            errors.add(new PartialError(null, throwable.getMessage()));
          } else {
            if (loadResult.getValue() != null) matchedEntities.add(loadResult.getValue());
          }
          promise.complete();
        });
    });

    CompositeFuture.join(multipleFutures)
      .onComplete(ar -> {
        String errorsAsStringJson = errors.encode();
        if (matchedEntities.size() == 0 && errors.size() == values.size()) {
          resultFuture.completeExceptionally(new MatchingException(errorsAsStringJson));
        } else {
          eventPayload.getContext().put(ERRORS, errorsAsStringJson);
          eventPayload.getContext().put(matchDetail.getExistingRecordType().value(), matchedEntities.encode());
          eventPayload.getContext().put(NOT_MATCHED_NUMBER, String.valueOf(values.size() - matchedEntities.size() - errors.size()));
          resultFuture.complete(matchedEntities.size() > 0);
        }
      });

    return resultFuture;
  }
}
