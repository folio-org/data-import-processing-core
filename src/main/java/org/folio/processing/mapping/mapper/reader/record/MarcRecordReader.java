package org.folio.processing.mapping.mapper.reader.record;

import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.value.BooleanValue;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.RepeatableFieldValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.RepeatableSubfieldMapping;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.marc4j.marc.Record;
import org.marc4j.marc.VariableField;
import org.marc4j.marc.impl.ControlFieldImpl;
import org.marc4j.marc.impl.DataFieldImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

@SuppressWarnings("all")
public class MarcRecordReader implements Reader {
  private final static Pattern MARC_PATTERN = Pattern.compile("(^[0-9]{3}(\\$[a-z]$){0,2})");
  private final static Pattern STRING_VALUE_PATTERN = Pattern.compile("(\"[^\"]+\")");
  private final static String WHITESPACE_DIVIDER = "\\s(?=(?:[^'\"`]*(['\"`])[^'\"`]*\\1)*[^'\"`]*$)";
  private final static String EXPRESSIONS_DIVIDER = "; else ";
  private final static String EXPRESSIONS_ARRAY = "[]";
  private final static String EXPRESSIONS_QUOTE = "\"";
  private static final Logger LOGGER = LoggerFactory.getLogger(MarcRecordReader.class);
  private EntityType entityType;
  private Record marcRecord;

  MarcRecordReader(EntityType entityType) {
    this.entityType = entityType;
  }

  @Override
  public void initialize(DataImportEventPayload eventPayload) {
    try {
      if (eventPayload.getContext() != null && eventPayload.getContext().containsKey(entityType.value())) {
        String stringRecord = eventPayload.getContext().get(entityType.value());
        org.folio.Record sourceRecord = new JsonObject(stringRecord).mapTo(org.folio.Record.class);
        if (sourceRecord != null
          && sourceRecord.getParsedRecord() != null
          && sourceRecord.getParsedRecord().getContent() != null) {
          MarcReader reader = buildMarcReader(sourceRecord);
          if (reader.hasNext()) {
            this.marcRecord = reader.next();
          } else {
            throw new IllegalArgumentException("Can not initialize MarcRecordReader, no suitable marc record found in event payload");
          }
        }
      } else {
        throw new IllegalArgumentException("Can not initialize MarcRecordReader, no suitable entity type found in event payload");
      }
    } catch (Exception e) {
      LOGGER.error("Can not read marc record from context", e);
      throw e;
    }
  }

  @Override
  public Value read(MappingRule ruleExpression) {
    try {
      if (ruleExpression.getBooleanFieldAction() != null) {
        return BooleanValue.of(ruleExpression.getBooleanFieldAction());
      } else if (ruleExpression.getSubfields().isEmpty() && isNotEmpty(ruleExpression.getValue())) {
        return readSingleField(ruleExpression);
      } else if (!ruleExpression.getSubfields().isEmpty() && ruleExpression.getRepeatableFieldAction() != null) {
        return readRepeatableField(ruleExpression);
      }
    } catch (Exception e) {
      LOGGER.error("Error during reading MappingRule expressions ", e);
    }
    return MissingValue.getInstance();
  }

  private Value readSingleField(MappingRule ruleExpression) {
    String[] expressions = ruleExpression.getValue().split(EXPRESSIONS_DIVIDER);
    boolean arrayValue = ruleExpression.getPath().endsWith(EXPRESSIONS_ARRAY);
    List<String> resultList = new ArrayList<>();
    for (String expression : expressions) {
      StringBuilder sb = new StringBuilder();
      String[] expressionParts = expression.split(WHITESPACE_DIVIDER);
      for (String expressionPart : expressionParts) {
        if (MARC_PATTERN.matcher(expressionPart).matches()) {
          List<String> marcValues = readValuesFromMarcRecord(expressionPart);
          if (arrayValue) {
            resultList.addAll(marcValues);
          } else {
            marcValues.forEach(v -> sb.append(v));
          }
        } else if (STRING_VALUE_PATTERN.matcher(expressionPart).matches()) {
          String value = expressionPart.replace(EXPRESSIONS_QUOTE, EMPTY);
          if (ruleExpression.getAcceptedValues() != null && !ruleExpression.getAcceptedValues().isEmpty()) {
            for (Map.Entry<String, String> entry : ruleExpression.getAcceptedValues().entrySet()) {
              if (entry.getValue().equals(value)) {
                value = entry.getKey();
              }
            }
          }
          if (arrayValue) {
            resultList.add(value);
          } else {
            sb.append(value);
          }
        }
      }
      resultList.remove(StringUtils.SPACE);
      if (arrayValue && !resultList.isEmpty()) {
        return ListValue.of(resultList);
      }
      if (isNotBlank(sb.toString())) {
        return StringValue.of(sb.toString());
      }
    }
    return MissingValue.getInstance();
  }

  private Value readRepeatableField(MappingRule ruleExpression) {
    List<RepeatableSubfieldMapping> subfields = ruleExpression.getSubfields();
    MappingRule.RepeatableFieldAction action = ruleExpression.getRepeatableFieldAction();
    List<Map<String, Value>> repeatableObject = new ArrayList<>();
    for (RepeatableSubfieldMapping subfieldMapping : subfields) {
      Map<String, Value> object = subfieldMapping.getFields()
        .stream()
        .map(mappingRule -> new ImmutablePair<>(mappingRule.getPath(), mappingRule.getBooleanFieldAction() != null
          ? BooleanValue.of(mappingRule.getBooleanFieldAction())
          : readSingleField(mappingRule))
        ).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
      repeatableObject.add(object);
    }
    return RepeatableFieldValue.of(repeatableObject, ruleExpression.getRepeatableFieldAction(), ruleExpression.getPath());
  }

  private List<String> readValuesFromMarcRecord(String marcPath) {
    List<VariableField> fields = marcRecord.getVariableFields(marcPath.substring(0, 3));
    LinkedHashSet<String> result = new LinkedHashSet<>();
    for (VariableField variableField : fields) {
      result.add(extractValueFromMarcRecord(variableField, marcPath));
    }
    return new ArrayList<>(result);
  }

  private String extractValueFromMarcRecord(VariableField field, String marcPath) {
    if (field instanceof DataFieldImpl) {
      return ((DataFieldImpl) field).getSubfieldsAsString(marcPath.substring(marcPath.length() - 1));
    } else if (field instanceof ControlFieldImpl) {
      return ((ControlFieldImpl) field).getData();
    }
    return EMPTY;
  }

  private MarcReader buildMarcReader(org.folio.Record record) {
    return new MarcJsonReader(new ByteArrayInputStream(
      record.getParsedRecord()
        .getContent()
        .toString()
        .getBytes(StandardCharsets.UTF_8)));
  }
}
