package org.folio.processing.matching.reader;

import java.util.Date;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.processing.value.DateValue;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.StaticValueDetails;

import static java.util.Objects.nonNull;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.STATIC_VALUE;

import io.netty.util.internal.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class StaticValueReaderImpl implements MatchValueReader {

  private static final String MAPPING_PARAMS = "MAPPING_PARAMS";
  private static final String RELATIONS = "MATCHING_PARAMETERS_RELATIONS";

  @Override
  public Value read(DataImportEventPayload eventPayload, MatchDetail matchDetail) {
    MatchExpression matchExpression = matchDetail.getIncomingMatchExpression();
    if (matchExpression.getDataValueType() == STATIC_VALUE && nonNull(matchExpression.getStaticValueDetails())) {
      StaticValueDetails staticValueDetails = matchExpression.getStaticValueDetails();
      switch (staticValueDetails.getStaticValueType()) {
        case TEXT: return obtainStringValue(matchDetail, eventPayload);
        case NUMBER: return obtainNumberValue(staticValueDetails.getNumber());
        case EXACT_DATE: return obtainDateValue(staticValueDetails.getExactDate(), staticValueDetails.getExactDate());
        case DATE_RANGE: return obtainDateValue(staticValueDetails.getFromDate(), staticValueDetails.getToDate());
        default: return MissingValue.getInstance();
      }
    }
    return MissingValue.getInstance();
  }

  @Override
  public boolean isEligibleForEntityType(EntityType incomingRecordType) {
    return incomingRecordType == EntityType.STATIC_VALUE;
  }

  private Value obtainStringValue(MatchDetail matchDetail, DataImportEventPayload eventPayload) {
    String id = retrieveIdFromContext(matchDetail, eventPayload);
    return nonNull(id) ? StringValue.of(id) : MissingValue.getInstance();
  }

  private Value obtainNumberValue(String value) {
    return nonNull(value) ? StringValue.of(value) : MissingValue.getInstance();
  }

  private Value obtainDateValue(Date from, Date to) {
    return nonNull(from) && nonNull(to) ? DateValue.of(from, to) : MissingValue.getInstance();
  }

  public static String retrieveIdFromContext(MatchDetail matchDetail, DataImportEventPayload eventPayload) {
    JsonObject matchingParams = new JsonObject(eventPayload.getContext().get(MAPPING_PARAMS));
    JsonObject relations = new JsonObject(eventPayload.getContext().get(RELATIONS));
    String relation = String.valueOf(relations.getJsonObject("matchingRelations")
      .getMap().get(matchDetail.getExistingMatchExpression().getFields().get(0).getValue()));
    JsonArray jsonArray = matchingParams.getJsonArray(relation);

    for (int i = 0; i < jsonArray.size(); i++) {
      if (jsonArray.getJsonObject(i).getString("name")
        .equals(matchDetail.getIncomingMatchExpression().getStaticValueDetails().getText().trim())) {
        JsonObject result = jsonArray.getJsonObject(i);
        return result.getString("id");
      }
    }
    eventPayload.getContext().remove(RELATIONS);
    return StringUtil.EMPTY_STRING;
  }
}
