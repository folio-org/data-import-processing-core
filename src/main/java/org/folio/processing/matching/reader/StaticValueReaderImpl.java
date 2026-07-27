package org.folio.processing.matching.reader;

import static java.util.Objects.nonNull;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.STATIC_VALUE;

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

public class StaticValueReaderImpl implements MatchValueReader {

  @Override
  public Value read(DataImportEventPayload eventPayload, MatchDetail matchDetail) {
    MatchExpression matchExpression = matchDetail.getIncomingMatchExpression();
    if (matchExpression.getDataValueType() == STATIC_VALUE && nonNull(matchExpression.getStaticValueDetails())) {
      StaticValueDetails staticValueDetails = matchExpression.getStaticValueDetails();
      return switch (staticValueDetails.getStaticValueType()) {
        case TEXT -> obtainStringValue(staticValueDetails.getText());
        case NUMBER -> obtainStringValue(staticValueDetails.getNumber());
        case EXACT_DATE -> obtainDateValue(staticValueDetails.getExactDate(), staticValueDetails.getExactDate());
        case DATE_RANGE -> obtainDateValue(staticValueDetails.getFromDate(), staticValueDetails.getToDate());
        default -> MissingValue.getInstance();
      };
    }
    return MissingValue.getInstance();
  }

  @Override
  public boolean isEligibleForEntityType(EntityType incomingRecordType) {
    return incomingRecordType == EntityType.STATIC_VALUE;
  }

  private Value obtainStringValue(String value) {
    return nonNull(value) ? StringValue.of(value) : MissingValue.getInstance();
  }

  private Value obtainDateValue(Date from, Date to) {
    return nonNull(from) && nonNull(to) ? DateValue.of(from, to) : MissingValue.getInstance();
  }
}
