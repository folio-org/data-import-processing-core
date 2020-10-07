package org.folio.processing.matching.reader;

import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.processing.matching.reader.util.MarcValueReaderUtil;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MatchExpression;

import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.EntityType.MARC_HOLDINGS;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;

/**
 * Implementation of MatchValueReader for MARC records
 */
public class MarcValueReaderImpl implements MatchValueReader {

  @Override
  public Value read(DataImportEventPayload eventPayload, MatchDetail matchDetail) {
    MatchExpression matchExpression = matchDetail.getIncomingMatchExpression();
    if (matchExpression.getDataValueType() == VALUE_FROM_RECORD) {
      String marcRecord = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
      return MarcValueReaderUtil.readValueFromRecord(marcRecord, matchExpression);
    }
    return MissingValue.getInstance();
  }

  @Override
  public boolean isEligibleForEntityType(EntityType incomingRecordType) {
    return incomingRecordType == MARC_BIBLIOGRAPHIC || incomingRecordType == MARC_AUTHORITY || incomingRecordType == MARC_HOLDINGS;
  }
}
