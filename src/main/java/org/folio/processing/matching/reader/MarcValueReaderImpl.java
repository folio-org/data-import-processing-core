package org.folio.processing.matching.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.Record;
import org.folio.processing.exceptions.ReaderException;
import org.folio.processing.matching.reader.util.MarcValueReaderUtil;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.folio.processing.matching.reader.util.MatchExpressionUtil.extractComparisonPart;
import static org.folio.processing.matching.reader.util.MatchExpressionUtil.isQualified;
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
