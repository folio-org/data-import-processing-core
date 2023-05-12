package org.folio.processing.matching.matcher;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.processing.exceptions.EventProcessingException;
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
  private static final String NOT_MATCHED_NUMBER = "NOT_MATCHED_NUMBER";

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
    List<String> matchedEntities = new ArrayList<>();
    List<String> errors = new ArrayList<>();

    List<Future> multipleFutures = new ArrayList<>();
    values.forEach(v -> {
      Promise<Void> promise = Promise.promise();
      multipleFutures.add(promise.future());
      loadEntity(v, matchDetail, eventPayload)
        .whenComplete((loadResult, throwable) -> {
          if (throwable != null) {
            errors.add(throwable.getMessage());
          } else {
            if (loadResult.getValue() != null) matchedEntities.add(loadResult.getValue());
          }
          promise.complete();
        });
    });

    CompositeFuture.join(multipleFutures)
      .onComplete(ar -> {
        String errorsAsStringJson = Json.encode(errors);
        if (matchedEntities.size() == 0 && errors.size() == values.size()) {
          resultFuture.completeExceptionally(new EventProcessingException(errorsAsStringJson));
        } else {
          eventPayload.getContext().put(ERRORS, errorsAsStringJson);
          eventPayload.getContext().put(matchDetail.getExistingRecordType().value(), Json.encode(matchedEntities));
          eventPayload.getContext().put(NOT_MATCHED_NUMBER, String.valueOf(values.size() - matchedEntities.size()));
          resultFuture.complete(matchedEntities.size() > 0);
        }
      });

    return resultFuture;
  }
}
