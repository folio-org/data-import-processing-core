package org.folio.processing.matching.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.exceptions.ReaderException;
import org.folio.processing.matching.model.schemas.Field;
import org.folio.processing.matching.model.schemas.MatchDetail;
import org.folio.processing.matching.model.schemas.MatchExpression;
import org.folio.processing.matching.model.schemas.MatchProfile;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.folio.processing.matching.model.schemas.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.processing.matching.model.schemas.MatchProfile.IncomingRecordType.MARC;

/**
 * Implementation of MatchValueReader for MARC records
 */
public class MarcValueReaderImpl implements MatchValueReader {

  private static final String MARC_FIELDS_POINTER = "/fields";
  private static final String MARC_SUBFIELDS_POINTER = "/subfields";
  private static final String MARC_IND_1_FIELD_NAME = "ind1";
  private static final String MARC_IND_2_FIELD_NAME = "ind2";
  private static final String FIELD_PROFILE_LABEL = "field";
  private static final String IND_1_PROFILE_LABEL = "indicator1";
  private static final String IND_2_PROFILE_LABEL = "indicator2";
  private static final String SUBFIELD_PROFILE_LABEL = "recordSubfield";

  @Override
  public Value read(EventContext context, MatchDetail matchDetail) {
    MatchExpression matchExpression = matchDetail.getIncomingMatchExpression();
    if (matchExpression.getDataValueType() == VALUE_FROM_RECORD) {
      String marcRecord = context.getObjects().get(MARC.value());
      return readValueFromRecord(marcRecord, matchExpression);
    }
    return MissingValue.getInstance();
  }

  @Override
  public boolean isEligibleForEntityType(MatchProfile.IncomingRecordType incomingRecordType) {
    return incomingRecordType == MARC;
  }

  private Value readValueFromRecord(String marcRecord, MatchExpression matchExpression) {
    if (StringUtils.isBlank(marcRecord)) {
      return MissingValue.getInstance();
    }

    Map<String, String> matchExpressionFields = getMatchExpressionFields(matchExpression.getFields());
    List<String> marcFieldValues = readMarcFieldValues(marcRecord, matchExpressionFields)
      .stream()
      .map(marcField -> readValue(marcField, matchExpressionFields))
      .filter(Optional::isPresent)
      .map(Optional::get)
      .collect(Collectors.toList());

    if (marcFieldValues.isEmpty()) {
      return MissingValue.getInstance();
    } else if (marcFieldValues.size() == 1) {
      return StringValue.of(marcFieldValues.get(0));
    } else {
      return ListValue.of(marcFieldValues);
    }
  }

  private Map<String, String> getMatchExpressionFields(List<Field> fields) {
    Map<String, String> resultMap = new HashMap<>();
    fields.forEach(field -> resultMap.put(field.getLabel(), field.getValue()));
    return resultMap;
  }

  private List<JsonNode> readMarcFieldValues(String marcRecord, Map<String, String> matchExpressionFields) {
    try {
      JsonNode fieldsNode = new ObjectMapper().readTree(marcRecord).at(MARC_FIELDS_POINTER);
      List<JsonNode> fields = fieldsNode.findValues(matchExpressionFields.get(FIELD_PROFILE_LABEL));
      return fields.stream()
        .filter(field -> field.isTextual() || isMatchingIdentifiers(field, matchExpressionFields))
        .collect(Collectors.toList());
    } catch (IOException e) {
      throw new ReaderException("Error reading MARC record", e);
    }
  }

  private Optional<String> readValue(JsonNode fieldValue, Map<String, String> matchExpressionFields) {
    if (fieldValue.isTextual()) {
      return Optional.of(fieldValue.textValue());
    }
    JsonNode subfields = fieldValue.at(MARC_SUBFIELDS_POINTER);
    JsonNode subfield = subfields.findValue(matchExpressionFields.get(SUBFIELD_PROFILE_LABEL));
    if (subfield != null && subfield.isTextual()) {
      return Optional.of(subfield.textValue());
    }
    return Optional.empty();
  }

  private boolean isMatchingIdentifiers(JsonNode field, Map<String, String> matchExpressionFields) {
    return field.findValue(MARC_IND_1_FIELD_NAME).textValue().equals(matchExpressionFields.get(IND_1_PROFILE_LABEL))
      && field.findValue(MARC_IND_2_FIELD_NAME).textValue().equals(matchExpressionFields.get(IND_2_PROFILE_LABEL));
  }

}