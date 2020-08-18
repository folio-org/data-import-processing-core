package org.folio.processing.matching.reader.util;

import org.folio.DataImportEventPayload;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Util class for processing and retrieving ids from context`s data.
 */
public final class MatchIdProcessorUtil {

  private static final String MAPPING_PARAMS = "MAPPING_PARAMS";
  private static final String RELATIONS = "MATCHING_PARAMETERS_RELATIONS";
  private static final String NAME_PROPERTY = "name";
  private static final String ID_PROPERTY = "id";
  private static final String CODE_PROPERTY = "code";
  private static final String LOCATIONS_PROPERTY = "locations";

  private MatchIdProcessorUtil() {
  }

  public static Value retrieveIdFromContext(String field, DataImportEventPayload eventPayload, Value value) {
    JsonObject matchingParams = new JsonObject(eventPayload.getContext().get(MAPPING_PARAMS));
    JsonObject relations = new JsonObject(eventPayload.getContext().get(RELATIONS));
    String relation = String.valueOf(relations.getJsonObject("matchingRelations")
      .getMap().get(field));
    JsonArray jsonArray = matchingParams.getJsonArray(relation);
    if (jsonArray == null) {
      return value;
    }
    if (relation.equals(LOCATIONS_PROPERTY)) {
      return checkMatchByLocation(jsonArray, String.valueOf(value.getValue()));
    }
    for (int i = 0; i < jsonArray.size(); i++) {
      if (jsonArray.getJsonObject(i).getString(NAME_PROPERTY)
        .equals(String.valueOf(value.getValue()))) {
        JsonObject result = jsonArray.getJsonObject(i);
        return StringValue.of(result.getString(ID_PROPERTY));
      }
    }
    eventPayload.getContext().remove(RELATIONS);
    return MissingValue.getInstance();
  }

  private static Value checkMatchByLocation(JsonArray jsonArray, String text) {
    for (int i = 0; i < jsonArray.size(); i++) {
      if (jsonArray.getJsonObject(i).getString(NAME_PROPERTY)
        .equals(text)
        || jsonArray.getJsonObject(i).getString(CODE_PROPERTY)
        .equals(text)
        || (String.format("%s (%s)", jsonArray.getJsonObject(i).getString(NAME_PROPERTY), jsonArray.getJsonObject(i).getString(CODE_PROPERTY)))
        .equals(text)) {
        JsonObject result = jsonArray.getJsonObject(i);
        return StringValue.of(result.getString(ID_PROPERTY));
      }
    }
    return MissingValue.getInstance();
  }
}
