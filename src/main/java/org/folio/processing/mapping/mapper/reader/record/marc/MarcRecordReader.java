package org.folio.processing.mapping.mapper.reader.record.marc;

import io.vertx.core.json.JsonObject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.matcher.AcceptedValuesMatcher;
import org.folio.processing.mapping.mapper.reader.matcher.StatisticalCodeAcceptedValuesMatcher;
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
import org.marc4j.marc.ControlField;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.VariableField;
import org.marc4j.marc.impl.ControlFieldImpl;
import org.marc4j.marc.impl.DataFieldImpl;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.processing.value.Value.ValueType.LIST;
import static org.folio.processing.value.Value.ValueType.MISSING;

@SuppressWarnings("all")
public class MarcRecordReader implements Reader {
  private static final Logger LOGGER = LogManager.getLogger(MarcRecordReader.class);

  public final static Pattern MARC_PATTERN = Pattern.compile("(^[0-9]{3}(\\$[a-z0-9]$){0,2})");
  private final static Pattern MARC_LEADER = Pattern.compile("^[LDR/]{4}[0-9-]{1,5}");
  private final static Pattern MARC_CONTROLLED = Pattern.compile("^[/0-9]{4}[0-9-]{1,5}");
  public final static Pattern STRING_VALUE_PATTERN = Pattern.compile("(\"[^\"]+\")");
  public final static String WHITESPACE_DIVIDER = "\\s(?=(?:[^'\"`]*(['\"`])[^'\"`]*\\1)*[^'\"`]*$)";
  public final static String EXPRESSIONS_DIVIDER = "; else ";
  private final static String EXPRESSIONS_ARRAY = "[]";
  private final static String EXPRESSIONS_QUOTE = "\"";
  private final static String MARC_SPLITTER = "/";
  private final static String MARC_BYTES_SPLITTER = "-";
  private final static String FIRST_BRACKET = "(";
  private final static String SECOND_BRACKET = ")";
  private static final String TODAY_PLACEHOLDER = "###TODAY###";
  private static final String REMOVE_PLACEHOLDER = "###REMOVE###";
  private static final String ISO_DATE_FORMAT = "yyyy-MM-dd";
  public static final String[] DATE_FORMATS = new String[]{ISO_DATE_FORMAT, "MM/dd/yyyy", "dd-MM-yyyy", "dd.MM.yyyy"};
  private static final String MAPPING_PARAMS = "MAPPING_PARAMS";
  private static final String TIMEZONE_PROPERTY = "timezone";
  private static final String DATE_TIME_FORMAT = "dd-MM-yyyy HH:mm:ss";
  private static final String UTC_TIMEZONE = "UTC";
  private static final List<String> NEEDS_VALIDATION_BY_ACCEPTED_VALUES = List.of("vendor", "materialSupplier", "accessProvider");
  private static final String STATISTICAL_CODE_ID_FIELD = "statisticalCodeId";
  private static final String BLANK = "";

  private EntityType entityType;
  private Map<String, AcceptedValuesMatcher> acceptedValuesMatchers;
  private Record marcRecord;
  private MappingParameters mappingParameters;

  MarcRecordReader(EntityType entityType) {
    this.entityType = entityType;
    this.acceptedValuesMatchers = Map.of(STATISTICAL_CODE_ID_FIELD, new StatisticalCodeAcceptedValuesMatcher());
  }

  @Override
  public void initialize(DataImportEventPayload eventPayload, MappingContext mappingContext) {
    try {
      if (eventPayload.getContext() != null && eventPayload.getContext().containsKey(entityType.value())) {
        this.mappingParameters = mappingContext.getMappingParameters();
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
      LOGGER.warn("initialize:: Can not read marc record from context", e);
      throw e;
    }
  }

  @Override
  public Value read(MappingRule ruleExpression) {
    try {
      if (ruleExpression.getBooleanFieldAction() != null) {
        return BooleanValue.of(ruleExpression.getBooleanFieldAction());
      } else if (ruleExpression.getSubfields().isEmpty() && isNotEmpty(ruleExpression.getValue())) {
        return readSingleField(ruleExpression, false);
      } else if (!ruleExpression.getSubfields().isEmpty() && ruleExpression.getRepeatableFieldAction() != null) {
        return readRepeatableField(ruleExpression);
      } else if (ruleExpression.getRepeatableFieldAction() == MappingRule.RepeatableFieldAction.DELETE_EXISTING) {
        return RepeatableFieldValue.of(Collections.emptyList(), ruleExpression.getRepeatableFieldAction(), ruleExpression.getPath());
      }
    } catch (Exception e) {
      LOGGER.warn("read:: Error during reading MappingRule expressions ", e);
    }
    return MissingValue.getInstance();
  }

  private Value readSingleField(MappingRule ruleExpression, boolean isRepeatableField) {
    if (ruleExpression.getValue() == null) {
      return MissingValue.getInstance();
    }

    String[] expressions = ruleExpression.getValue().split(EXPRESSIONS_DIVIDER);
    boolean arrayValue = ruleExpression.getPath().endsWith(EXPRESSIONS_ARRAY);
    List<String> resultList = new ArrayList<>();
    for (String expression : expressions) {
      StringBuilder sb = new StringBuilder();
      StringBuilder sbForMultiple = new StringBuilder();
      String[] expressionParts = expression.split(WHITESPACE_DIVIDER);
      for (String expressionPart : expressionParts) {
        if (MARC_PATTERN.matcher(expressionPart).matches()
          || (MARC_CONTROLLED.matcher(expressionPart).matches())
          || (MARC_LEADER.matcher(expressionPart).matches())) {
          processMARCExpression(arrayValue, isRepeatableField, resultList, sb, sbForMultiple, expressionPart, ruleExpression);
        } else if (STRING_VALUE_PATTERN.matcher(expressionPart).matches()) {
          sbForMultiple = processStringExpression(ruleExpression, arrayValue, resultList, sb, sbForMultiple, expressionPart);
        } else if (TODAY_PLACEHOLDER.equalsIgnoreCase(expressionPart)) {
          processTodayExpression(sb, sbForMultiple);
        } else if (REMOVE_PLACEHOLDER.equalsIgnoreCase(expressionPart)) {
          return StringValue.of(expressionPart, true);
        }
      }
      resultList = resultList.stream().filter(r -> isNotBlank(r)).collect(Collectors.toList());
      List<String> tmpResultList = new ArrayList<>(resultList);
      String concatenator = sb.toString();
      if (isNotBlank(concatenator)) {
        for (int i = 0; i < tmpResultList.size(); i++) {
          String element = tmpResultList.get(i);
          resultList.set(i, element.concat(concatenator).toString());
        }
      }
      if ((arrayValue || isRepeatableField) && !resultList.isEmpty()) {
        return ListValue.of(resultList);
      }
      if (isNotBlank(sb.toString())) {
        return StringValue.of(sb.toString());
      }
    }
    return MissingValue.getInstance();
  }

  /**
   * Process marc expression method
   *
   * @param arrayValue            if arrayValue process as array
   * @param isRepeatableField     if isRepeatableField process as repeatable field
   * @param resultList            resultList uses for saving result
   * @param sb                    uses for appending single marc to buffer
   * @param multipleStringBuilder uses for appending to results from buffer
   * @param expressionPart        this String must be marc uses for serching values in marcRecord
   * @param ruleExpression        uses for mapping values before processing
   */
  private void processMARCExpression(boolean arrayValue, boolean isRepeatableField, List<String> resultList, StringBuilder sb, StringBuilder multipleStringBuilder, String expressionPart, MappingRule ruleExpression) {
    List<String> marcValues = readValuesFromMarcRecord(expressionPart).stream().filter(m -> isNotBlank(m)).collect(Collectors.toList());
    if (arrayValue || (isRepeatableField && marcValues.size() > 1)) {
      if (resultList.size() > 1 && marcValues.size() == resultList.size()) {
        List<String> collectedValues = marcValues.stream().map(value -> getFromAcceptedValues(ruleExpression, value)).collect(Collectors.toList());
        List<String> tmpResultList = new ArrayList<>(resultList);
        String concatenator = multipleStringBuilder.toString();
        for (int i = 0; i < tmpResultList.size(); i++) {
          String element = tmpResultList.get(i);
          resultList.set(i, element.concat(concatenator + collectedValues.get(i)).toString());
        }
      } else {
        // TODO This todo for cases where first subfields count not equals second subfields count
        List<String> collectedValues = marcValues.stream().map(value -> getFromAcceptedValues(ruleExpression, value)).collect(Collectors.toList());
        resultList.addAll(collectedValues);
      }
    } else {
      if (!marcValues.isEmpty()) {
        sb.append(getFromAcceptedValues(ruleExpression, marcValues.get(0)));
      }
    }
  }

  private String getFromAcceptedValues(MappingRule ruleExpression, String value) {
    AcceptedValuesMatcher acceptedValuesMatcher = Objects.isNull(ruleExpression.getName()) ? null
      : acceptedValuesMatchers.get(ruleExpression.getName());

    if (ruleExpression.getAcceptedValues() != null && !ruleExpression.getAcceptedValues().isEmpty()) {
      for (Map.Entry<String, String> entry : ruleExpression.getAcceptedValues().entrySet()) {
        if ((acceptedValuesMatcher != null && acceptedValuesMatcher.matches(entry.getValue(), value))
          || entry.getValue().equalsIgnoreCase(value) || equalsBasedOnBrackets(entry.getValue(), value)) {
          value = entry.getKey();
        }
      }
    }
    boolean needsValidationByAcceptedValues = NEEDS_VALIDATION_BY_ACCEPTED_VALUES.contains(String.valueOf(ruleExpression.getName()));

    if (needsValidationByAcceptedValues && !ruleExpression.getAcceptedValues().containsKey(value)) {
      return BLANK;
    }

    return value;
  }

  private boolean equalsBasedOnBrackets(String mappingParameter, String value) {
    if (mappingParameter.contains(FIRST_BRACKET) && mappingParameter.contains(SECOND_BRACKET)) {
      if (retrieveStringFromLastBrackets(mappingParameter).equalsIgnoreCase(value)) {
        return true;
      } else if (retrieveStringWithBracketsFromLastOne(mappingParameter).equalsIgnoreCase(value)) {
        return true;
      } else if (retrieveNameWithoutCode(mappingParameter).equalsIgnoreCase(value)) {
        return true;
      }
      return false;
    }
    return false;
  }

  private String retrieveStringFromLastBrackets(String mappingParameter) {
    return mappingParameter.substring(mappingParameter.lastIndexOf(FIRST_BRACKET) + 1, mappingParameter.lastIndexOf(SECOND_BRACKET));
  }

  private String retrieveStringWithBracketsFromLastOne(String mappingParameter) {
    return mappingParameter.substring(mappingParameter.indexOf(FIRST_BRACKET), mappingParameter.indexOf(SECOND_BRACKET) + 1);
  }

  private String retrieveNameWithoutCode(String mappingParameter) {
    return mappingParameter.substring(0, mappingParameter.trim().indexOf(FIRST_BRACKET) - 1);
  }

  /**
   * Process string expression method
   *
   * @param arrayValue            if arrayValue process as array
   * @param resultList            resultList uses for saving result
   * @param sb                    uses for appending value to buffer
   * @param multipleStringBuilder uses for appending to results from buffer
   * @param expressionPart        this String must be marc uses for serching values in marcRecord
   * @param ruleExpression        uses for mapping values before processing
   * @return empty StringBuilder if arrayValue or
   */
  private StringBuilder processStringExpression(MappingRule ruleExpression, boolean arrayValue, List<String> resultList, StringBuilder sb, StringBuilder multipleStringBuilder, String expressionPart) {
    String value = expressionPart.replace(EXPRESSIONS_QUOTE, EMPTY);
    value = getFromAcceptedValues(ruleExpression, value);
    if (isNotEmpty(value)) {
      if (arrayValue) {
        resultList.add(value);
      } else {
        sb.append(value);
        return new StringBuilder(value);
      }
    }
    return new StringBuilder(EMPTY);
  }

  /**
   * Process TODAY expression method
   * appends ZonedDateTime.now for tenant
   *
   * @param sb                    uses for appending today value to buffer
   * @param multipleStringBuilder uses for appending today value to buffer
   * @throws IllegalArgumentException if can not format today
   */
  private void processTodayExpression(StringBuilder sb, StringBuilder multipleStringBuilder) {
    try {
      DateTimeFormatter isoFormatter = DateTimeFormatter.ofPattern(ISO_DATE_FORMAT);
      String tenantConfiguration = this.mappingParameters.getTenantConfiguration();
      String tenantTimezone;
      if (isTimezoneParameterIsEmpty(tenantConfiguration)) {
        tenantTimezone = UTC_TIMEZONE; // default if timezone configuration is empty.
      } else {
        tenantTimezone = new JsonObject(tenantConfiguration).getString(TIMEZONE_PROPERTY);
      }
      ZonedDateTime utcZonedDateTime = ZonedDateTime.now(ZoneId.of(tenantTimezone));
      sb.append(isoFormatter.format(utcZonedDateTime));
      multipleStringBuilder.append(isoFormatter.format(utcZonedDateTime));
    } catch (Exception e) {
      LOGGER.warn("processTodayExpression:: Can not process ##TODAY## expression", e);
      throw new IllegalArgumentException("Can not process ##TODAY## expression", e);
    }
  }

  private boolean isTimezoneParameterIsEmpty(String tenantConfiguration) {
    return tenantConfiguration == null || tenantConfiguration.isBlank()
      || new JsonObject(tenantConfiguration).getString(TIMEZONE_PROPERTY) == null
      || new JsonObject(tenantConfiguration).getString(TIMEZONE_PROPERTY).isBlank();
  }

  private Value readRepeatableField(MappingRule ruleExpression) {
    List<RepeatableSubfieldMapping> subfields = ruleExpression.getSubfields();
    MappingRule.RepeatableFieldAction action = ruleExpression.getRepeatableFieldAction();
    boolean isRepeatableField = true;
    List<Map<String, Value>> repeatableObject = new ArrayList<>();
    List<String> repeatableStrings = new ArrayList<>();

    for (RepeatableSubfieldMapping subfieldMapping : subfields) {
      List<Map<String, Value>> repeatableObjectItems = new ArrayList<>();
      HashMap<String, Value> object = new HashMap<>();
      repeatableObjectItems.add(object);
      for (MappingRule mappingRule : subfieldMapping.getFields()) {
        if (subfieldMapping.getPath().equals(mappingRule.getPath())) {
          if (STRING_VALUE_PATTERN.matcher(mappingRule.getValue()).matches()) {
            repeatableStrings.add(readRepeatableStringField(mappingRule));
          } else {
            retrieveValuesFromMarcRecord(repeatableStrings, mappingRule);
          }
        } else {
          Value value = mappingRule.getBooleanFieldAction() != null
            ? BooleanValue.of(mappingRule.getBooleanFieldAction())
            : readSingleField(mappingRule, isRepeatableField);

          if (value.getType() == MISSING && mappingRule.getRequired()) {
            repeatableObjectItems.remove(repeatableObjectItems.size() - 1);
            break;
          } else if (shouldCreateItemPerRepeatedMarcField(value.getType(), mappingRule)) {
            ListValue listValue = (ListValue) value;
            ensureRepeatableObjectItemsAmount(repeatableObjectItems, listValue.getValue().size());
            fillInRepeatableObjectItemsWithValue(repeatableObjectItems, mappingRule.getPath(), listValue);
          } else {
            object.put(mappingRule.getPath(), value);
          }
        }
      }
      fillInRepeatableFieldItemsWithMissedProperties(repeatableObjectItems);
      repeatableObject.addAll(repeatableObjectItems);
    }
    return repeatableStrings.isEmpty() ? RepeatableFieldValue.of(repeatableObject, action, ruleExpression.getPath())
      : ListValue.of(repeatableStrings, ruleExpression.getRepeatableFieldAction());
  }

  private void retrieveValuesFromMarcRecord(List<String> repeatableStrings, MappingRule mappingRule) {
    Value valueFromMarcFile = readSingleField(mappingRule, false);
    if (valueFromMarcFile != null && valueFromMarcFile.getType() == LIST) {
      for (String stringValue : (List<String>) valueFromMarcFile.getValue()) {
        repeatableStrings.add(stringValue);
      }
    }
  }

  private String readRepeatableStringField(MappingRule mappingRule) {
    String value = mappingRule.getValue().replace(EXPRESSIONS_QUOTE, EMPTY);
    if (MapUtils.isNotEmpty(mappingRule.getAcceptedValues())) {
      for (Map.Entry<String, String> entry : mappingRule.getAcceptedValues().entrySet()) {
        if (entry.getValue().equals(value)) {
          value = entry.getKey();
        }
      }
    }
    return value;
  }

  private boolean shouldCreateItemPerRepeatedMarcField(Value.ValueType valueType, MappingRule mappingRule) {
    return valueType == Value.ValueType.LIST && !mappingRule.getPath().endsWith(EXPRESSIONS_ARRAY);
  }

  private void ensureRepeatableObjectItemsAmount(List<Map<String, Value>> repeatableObjectItems, int necessaryAmount) {
    int newItemsAmount = necessaryAmount - repeatableObjectItems.size();
    for (int i = 0; i < newItemsAmount; i++) {
      HashMap<String, Value> itemCopyWtihPreviousProperties = new HashMap<>(repeatableObjectItems.get(0));
      repeatableObjectItems.add(itemCopyWtihPreviousProperties);
    }
  }

  private void fillInRepeatableObjectItemsWithValue(List<Map<String, Value>> repeatableObjectItems, String path, ListValue value) {
    List<String> values = (List<String>) value.getValue();
    for (int i = 0; i < values.size(); i++) {
      repeatableObjectItems.get(i).put(path, StringValue.of(values.get(i)));
    }
  }

  private void fillInRepeatableFieldItemsWithMissedProperties(List<Map<String, Value>> repeatableFieldItems) {
    if (!repeatableFieldItems.isEmpty()) {
      Map<String, Value> firstRepeatableFieldItem = repeatableFieldItems.get(0);
      Set<Map.Entry<String, Value>> propertiesToFillIn = firstRepeatableFieldItem.entrySet();
      for (Map.Entry<String, Value> property : propertiesToFillIn) {
        for (int i = 1; i < repeatableFieldItems.size(); i++) {
          Map<String, Value> itemToFillProperty = repeatableFieldItems.get(i);
          if (itemToFillProperty.get(property.getKey()) == null) {
            itemToFillProperty.put(property.getKey(), property.getValue());
          }
        }
      }
    }
  }

  private List<String> readValuesFromMarcRecord(String marcPath) {
    List<String> results = new ArrayList<>();
    if (MARC_PATTERN.matcher(marcPath).matches()) {
      List<VariableField> fields = marcRecord.getVariableFields(marcPath.substring(0, 3));
      List<String> result = new LinkedList<>();
      for (VariableField variableField : fields) {
        result.addAll(extractValueFromMarcRecord(variableField, marcPath));
      }
      List<String> distinctResult = result.stream().distinct().collect(Collectors.toList());
      if (distinctResult.size() > 1 && distinctResult.size() != fields.size()) {
        results.addAll(result);
      } else {
        results.addAll(distinctResult);
      }
    } else if ((MARC_CONTROLLED.matcher(marcPath).matches())) {
      String controllFieldTag = StringUtils.substringBefore(marcPath, MARC_SPLITTER);
      Optional<ControlField> controlField = marcRecord.getControlFields().stream()
        .filter(cf -> cf.getTag().equals(controllFieldTag))
        .findFirst();
      if (controlField.isPresent()) {
        String data = controlField.get().getData();
        results.add(getDataFromToExpression(data, marcPath));
      }
    } else if ((MARC_LEADER.matcher(marcPath).matches())) {
      results.add(getDataFromToExpression(marcRecord.getLeader().marshal(), marcPath));
    }
    return results;
  }

  private String getDataFromToExpression(String data, String marcPath) {
    String marcPathParts = StringUtils.substringAfter(marcPath, MARC_SPLITTER);
    String[] fromTo = marcPathParts.split(MARC_BYTES_SPLITTER);
    int from = Integer.parseInt(fromTo[0]) - 1;
    int to = Integer.parseInt(fromTo.length > 1 ? fromTo[1] : String.valueOf(from + 1));
    return data.substring(from, to > data.length() - 1 ? data.length() - 1 : to);
  }

  private List<String> extractValueFromMarcRecord(VariableField field, String marcPath) {
    List<String> value = new ArrayList<>();
    if (field instanceof DataFieldImpl) {
      ((DataFieldImpl) field).getSubfields(marcPath.substring(marcPath.length() - 1))
        .stream()
        .map(Subfield::getData)
        .map(this::formatToIsoDate)
        .forEach(v -> value.add(v));
    } else if (field instanceof ControlFieldImpl) {
      value.add(((ControlFieldImpl) field).getData());
    }
    return value;
  }

  private String formatToIsoDate(String stringToFormat) {
    try {
      if (isNotEmpty(stringToFormat)) {
        DateFormat df = new SimpleDateFormat(ISO_DATE_FORMAT);
        return df.format(parseDate(stringToFormat));
      }
      return stringToFormat;
    } catch (ParseException e) {
      return stringToFormat;
    }
  }

  private Date parseDate(String stringToFormat) throws ParseException {
    SimpleDateFormat parser = null;
    ParsePosition pos = new ParsePosition(0);
    for (int i = 0; i < DATE_FORMATS.length; i++) {
      if (i == 0) {
        parser = new SimpleDateFormat(DATE_FORMATS[0]);
        parser.setLenient(false);
      } else {
        parser.applyPattern(DATE_FORMATS[i]);
      }
      pos.setIndex(0);
      Date date = parser.parse(stringToFormat, pos);
      if (date != null && pos.getIndex() == stringToFormat.length()) {
        return date;
      }
    }
    throw new ParseException("Unable to parse the date: " + stringToFormat, -1);
  }

  private MarcReader buildMarcReader(org.folio.Record record) {
    return new MarcJsonReader(new ByteArrayInputStream(
      record.getParsedRecord()
        .getContent()
        .toString()
        .getBytes(StandardCharsets.UTF_8)));
  }
}
