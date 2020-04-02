package org.folio.processing.matching.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.Record;
import org.folio.processing.exceptions.ReaderException;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;

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

  private static final String MARC_FIELDS_POINTER = "/fields";
  private static final String MARC_SUBFIELDS_POINTER = "/subfields";
  private static final String MARC_IND_1_FIELD_NAME = "ind1";
  private static final String MARC_IND_2_FIELD_NAME = "ind2";
  private static final String FIELD_PROFILE_LABEL = "field";
  private static final String IND_1_PROFILE_LABEL = "indicator1";
  private static final String IND_2_PROFILE_LABEL = "indicator2";
  private static final String SUBFIELD_PROFILE_LABEL = "recordSubfield";

  @Override
  public Value read(DataImportEventPayload eventPayload, MatchDetail matchDetail) {
    MatchExpression matchExpression = matchDetail.getIncomingMatchExpression();
    if (matchExpression.getDataValueType() == VALUE_FROM_RECORD) {
      String marcRecord = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
      return readValueFromRecord(marcRecord, matchExpression);
    }
    return MissingValue.getInstance();
  }

  @Override
  public boolean isEligibleForEntityType(EntityType incomingRecordType) {
    return incomingRecordType == MARC_BIBLIOGRAPHIC || incomingRecordType == MARC_AUTHORITY || incomingRecordType == MARC_HOLDINGS;
  }

  private Value readValueFromRecord(String marcRecord, MatchExpression matchExpression) {
    if (StringUtils.isBlank(marcRecord)) {
      return MissingValue.getInstance();
    }

    Map<String, String> matchExpressionFields = getMatchExpressionFields(matchExpression.getFields());
    List<String> marcFieldValues = readMarcFieldValues(marcRecord, matchExpressionFields)
      .stream()
      .map(marcField -> readValues(marcField, matchExpressionFields))
      .flatMap(List::stream)
      .filter(value -> isQualified(value, matchExpression.getQualifier()))
      .map(value -> extractComparisonPart(value, matchExpression.getQualifier()))
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
      Record record = new ObjectMapper().readValue(marcRecord, Record.class);
      String parsedContent = record.getParsedRecord().getContent().toString();
      JsonNode fieldsNode = new ObjectMapper().readTree(parsedContent).at(MARC_FIELDS_POINTER);
      List<JsonNode> fields = fieldsNode.findValues(matchExpressionFields.get(FIELD_PROFILE_LABEL));
      return fields.stream()
        .filter(field -> field.isTextual() || isMatchingIdentifiers(field, matchExpressionFields))
        .collect(Collectors.toList());
    } catch (IOException e) {
      throw new ReaderException("Error reading MARC record", e);
    }
  }

  private List<String> readValues(JsonNode fieldValue, Map<String, String> matchExpressionFields) {
    if (fieldValue.isTextual()) {
      return Collections.singletonList(fieldValue.textValue());
    }
    JsonNode subfields = fieldValue.at(MARC_SUBFIELDS_POINTER);
    return subfields.findValues(matchExpressionFields.get(SUBFIELD_PROFILE_LABEL)).stream()
      .filter(JsonNode::isTextual)
      .map(JsonNode::textValue)
      .collect(Collectors.toList());
  }

  private boolean isMatchingIdentifiers(JsonNode field, Map<String, String> matchExpressionFields) {
    return field.findValue(MARC_IND_1_FIELD_NAME).textValue().equals(matchExpressionFields.get(IND_1_PROFILE_LABEL))
      && field.findValue(MARC_IND_2_FIELD_NAME).textValue().equals(matchExpressionFields.get(IND_2_PROFILE_LABEL));
  }

}
