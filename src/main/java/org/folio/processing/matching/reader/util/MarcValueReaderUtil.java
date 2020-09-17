package org.folio.processing.matching.reader.util;

import static org.folio.processing.matching.reader.util.MatchExpressionUtil.extractComparisonPart;
import static org.folio.processing.matching.reader.util.MatchExpressionUtil.isQualified;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.folio.processing.exceptions.ReaderException;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class MarcValueReaderUtil {

  private static final String MARC_FIELDS_POINTER = "/fields";
  private static final String MARC_SUBFIELDS_POINTER = "/subfields";
  private static final String MARC_IND_1_FIELD_NAME = "ind1";
  private static final String MARC_IND_2_FIELD_NAME = "ind2";
  private static final String FIELD_PROFILE_LABEL = "field";
  private static final String IND_1_PROFILE_LABEL = "indicator1";
  private static final String IND_2_PROFILE_LABEL = "indicator2";
  private static final String SUBFIELD_PROFILE_LABEL = "recordSubfield";
  private static final String ASTERISK_INDICATOR = "*";

  private MarcValueReaderUtil() {
  }

  public static Value readValueFromRecord(String marcRecord, MatchExpression matchExpression) {
    if (org.apache.commons.lang3.StringUtils.isBlank(marcRecord)) {
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

  private static Map<String, String> getMatchExpressionFields(List<Field> fields) {
    Map<String, String> resultMap = new HashMap<>();
    fields.forEach(field -> resultMap.put(field.getLabel(), field.getValue()));
    return resultMap;
  }

  private static List<JsonNode> readMarcFieldValues(String marcRecord, Map<String, String> matchExpressionFields) {
    try {
      org.folio.Record record = new ObjectMapper().readValue(marcRecord, org.folio.Record.class);
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

  private static List<String> readValues(JsonNode fieldValue, Map<String, String> matchExpressionFields) {
    if (fieldValue.isTextual()) {
      return Collections.singletonList(fieldValue.textValue());
    }
    JsonNode subfields = fieldValue.at(MARC_SUBFIELDS_POINTER);
    return subfields.findValues(matchExpressionFields.get(SUBFIELD_PROFILE_LABEL)).stream()
      .filter(JsonNode::isTextual)
      .map(JsonNode::textValue)
      .collect(Collectors.toList());
  }

  private static boolean isMatchingIdentifiers(JsonNode field, Map<String, String> matchExpressionFields) {
    boolean isFirstIndicatorMatched = isIndicatorMatched(field, matchExpressionFields, IND_1_PROFILE_LABEL, MARC_IND_1_FIELD_NAME);
    boolean isSecondIndicatorMatched = isIndicatorMatched(field, matchExpressionFields, IND_2_PROFILE_LABEL, MARC_IND_2_FIELD_NAME);
    return isFirstIndicatorMatched && isSecondIndicatorMatched;
  }

  private static boolean isIndicatorMatched(JsonNode field, Map<String, String> matchExpressionFields,
                                            String indicatorProfile, String indicatorFieldName) {
    boolean isIndicatorMatched = false;
    if (matchExpressionFields.get(indicatorProfile).equals(ASTERISK_INDICATOR)) {
      isIndicatorMatched = true;
    }
    if (matchExpressionFields.get(indicatorProfile).trim().equals(org.apache.commons.lang3.StringUtils.EMPTY)) {
      isIndicatorMatched = field.findValue(indicatorFieldName).textValue().equals(org.apache.commons.lang3.StringUtils.SPACE);
    }
    if (field.findValue(indicatorFieldName).textValue().equals(matchExpressionFields.get(indicatorProfile))) {
      isIndicatorMatched = true;
    }
    return isIndicatorMatched;
  }
}
