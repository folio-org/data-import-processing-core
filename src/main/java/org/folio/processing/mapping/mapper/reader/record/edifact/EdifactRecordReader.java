package org.folio.processing.mapping.mapper.reader.record.edifact;

import com.google.common.collect.Iterables;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataElement;
import org.folio.DataImportEventPayload;
import org.folio.EdifactParsedContent;
import org.folio.Record;
import org.folio.Segment;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNoneBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * The {@link Reader} implementation for EDIFACT INVOICE.
 * Reads {@link Value} by rule from EDIFACT parsed content.
 */
public class EdifactRecordReader implements Reader {

  private static final Logger LOGGER = LoggerFactory.getLogger(EdifactRecordReader.class);

  private static final Pattern CONSTANT_EXPRESSION_PATTERN = Pattern.compile("(\"[^\"]+\")");
  private static final Pattern SEGMENT_QUERY_PATTERN = Pattern.compile("[A-Z]{3}((\\+|<)\\w*)(\\2*\\w*)*(\\?\\w+)?\\[[1-9](-[1-9])?\\]");
  private static final String EXPRESSIONS_DELIMITER = "; else ";
  private static final String QUALIFIER_SIGN = "?";
  private static final String QUOTATION_MARK = "\"";
  private static final int SEGMENT_TAG_LENGTH = 3;
  public static final String INVALID_MAPPING_EXPRESSION_MSG = "The specified mapping expression '%s' is invalid";

  private EntityType entityType;
  private EdifactParsedContent edifactParsedContent;
  private List<Segment> invoiceSegments;
  private List<List<Segment>> invoiceLinesSegmentGroups;

  public EdifactRecordReader(EntityType entityType) {
    this.entityType = entityType;
  }

  @Override
  public void initialize(DataImportEventPayload eventPayload) throws IOException {
    if (eventPayload.getContext() != null && isNotBlank(eventPayload.getContext().get(entityType.value()))) {
      String recordAsString = eventPayload.getContext().get(entityType.value());
      Record sourceRecord = Json.decodeValue(recordAsString, Record.class);
      if (ObjectUtils.allNotNull(sourceRecord.getParsedRecord(), sourceRecord.getParsedRecord().getContent())) {
        edifactParsedContent = DatabindCodec.mapper().readValue(sourceRecord.getParsedRecord().getContent().toString(), EdifactParsedContent.class);
        invoiceSegments = getInvoiceSegments();
        invoiceLinesSegmentGroups = getInvoiceLinesSegments();
        return;
      }
    }
    throw new IllegalArgumentException("Can not initialize EdifactRecordReader, event payload has no EDIFACT parsed content");
  }

  private List<Segment> getInvoiceSegments() {
    List<Segment> segments = edifactParsedContent.getSegments();
    int invoiceHeaderSegmentsEnd = 0;
    int invoiceSummarySegmentsStart = 0;

    for (int i = 0; i < segments.size(); i++) {
      Segment segment = segments.get(i);
      if ("LIN".equals(segment.getTag())) {
        invoiceHeaderSegmentsEnd = i - 1;
      } else if ("UNS".equals(segment.getTag())) {
        invoiceSummarySegmentsStart = i;
        break;
      }
    }
    ArrayList<Segment> invoiceSegments = new ArrayList<>(segments.subList(0, invoiceHeaderSegmentsEnd));
    invoiceSegments.addAll(segments.subList(invoiceSummarySegmentsStart, segments.size()));
    return invoiceSegments;
  }

  private List<List<Segment>> getInvoiceLinesSegments() {
    List<List<Segment>> invoiceLinesSegments = new ArrayList<>();
    int invoiceLineStart = 0;
    int invoiceLineEndExclusive;
    List<Segment> segments = edifactParsedContent.getSegments();

    for (int i = 0; i < segments.size(); i++) {
      Segment segment = segments.get(i);
      if ("LIN".equals(segment.getTag())) {
        if (invoiceLineStart != 0) {
          invoiceLineEndExclusive = i;
          invoiceLinesSegments.add(segments.subList(invoiceLineStart, invoiceLineEndExclusive));
        }
        invoiceLineStart = i;
      } else if ("UNS".equals(segment.getTag())) {
        invoiceLineEndExclusive = i;
        invoiceLinesSegments.add(segments.subList(invoiceLineStart, invoiceLineEndExclusive));
        break;
      }
    }
    return invoiceLinesSegments;
  }

  @Override
  public Value read(MappingRule mappingRule) {
    if (mappingRule.getPath().contains("invoiceLine")) {
      return readInvoiceLinesRepeatableFieldValue(mappingRule);
    }
    return read(mappingRule, invoiceSegments);
  }

  private Value read(MappingRule mappingRule, List<Segment> segments) {
    if (mappingRule.getBooleanFieldAction() != null) {
      // todo: create task for ui to add "booleanFieldAction" : "ALL_TRUE" to the rule
      return BooleanValue.of(mappingRule.getBooleanFieldAction());
    } else if (mappingRule.getSubfields().isEmpty()) {
      return readSingleFieldValue(mappingRule, segments);
    } else if (isListValueMappingRule(mappingRule)) {
      return readListValue(mappingRule);
    } else if (!mappingRule.getSubfields().isEmpty()) {
      return readRepeatableFieldValue(mappingRule, segments);
    }
    return MissingValue.getInstance();
  }

  private RepeatableFieldValue readInvoiceLinesRepeatableFieldValue(MappingRule mappingRule) {
    List<Map<String, Value>> repeatableObjects = new ArrayList<>();
    for (List<Segment> invoiceLineSegments : invoiceLinesSegmentGroups) {
      HashMap<String, Value> objectModel = new HashMap<>();
      for (RepeatableSubfieldMapping repeatableObjectRule : mappingRule.getSubfields()) {
        for (MappingRule fieldRule : repeatableObjectRule.getFields()) {
          Value value = read(fieldRule, invoiceLineSegments);
          objectModel.put(fieldRule.getPath(), value);
        }
      }
      repeatableObjects.add(objectModel);
    }
    return RepeatableFieldValue.of(repeatableObjects, mappingRule.getRepeatableFieldAction(), mappingRule.getPath());
  }

  private Value readRepeatableFieldValue(MappingRule mappingRule, List<Segment> segments) {
    MappingRule.RepeatableFieldAction action = mappingRule.getRepeatableFieldAction();
    List<Map<String, Value>> repeatableObjects = new ArrayList<>();

    for (RepeatableSubfieldMapping elementRule : mappingRule.getSubfields()) {
      HashMap<String, Value> objectModel = new HashMap<>();
      for (MappingRule fieldRule : elementRule.getFields()) {
        Value readValue = read(fieldRule, segments);
        objectModel.put(fieldRule.getPath(), readValue);
      }
      repeatableObjects.add(objectModel);
    }
    return RepeatableFieldValue.of(repeatableObjects, action, mappingRule.getPath());
  }

  private Value readSingleFieldValue(MappingRule mappingRule, List<Segment> invoiceLineSegments) {
    String readValue;
    String mappingExpression = mappingRule.getValue();
    String[] expressionParts = mappingExpression.split(EXPRESSIONS_DELIMITER);

    for (String expressionPart : expressionParts) {
      if (CONSTANT_EXPRESSION_PATTERN.matcher(expressionPart).matches()) {
        readValue = readAcceptableValue(mappingRule);
      } else if (SEGMENT_QUERY_PATTERN.matcher(expressionPart).matches()) {
        List<String> segmentsData = extractSegmentsData(expressionPart, invoiceLineSegments);
        readValue = String.join(EMPTY, segmentsData);
      } else {
        String msg = format(INVALID_MAPPING_EXPRESSION_MSG, expressionPart);
        LOGGER.error(msg);
        throw new IllegalArgumentException(msg);
      }

      if (isNoneBlank(readValue)) {
        return StringValue.of(readValue);
      }
    }
    return MissingValue.getInstance();
  }

  private boolean isListValueMappingRule(MappingRule mappingRule) {
    return mappingRule.getSubfields().stream()
      .map(subfieldRule -> subfieldRule.getFields().size() == 1)
      .reduce((x, y) -> x & y)
      .orElse(false);
  }

  private ListValue readListValue(MappingRule mappingRule) {
    List<String> values = new ArrayList<>();
    for (RepeatableSubfieldMapping elementRule : mappingRule.getSubfields()) {
      for (MappingRule fieldRule : elementRule.getFields()) {
        values.add(readAcceptableValue(fieldRule));
      }
    }
    return ListValue.of(values);
  }

  private String readAcceptableValue(MappingRule mappingRule) {
    String value = StringUtils.substringBetween(mappingRule.getValue(), QUOTATION_MARK);
    if (MapUtils.isNotEmpty(mappingRule.getAcceptedValues())) {
      for (Map.Entry<String, String> entry : mappingRule.getAcceptedValues().entrySet()) {
        if (entry.getValue().equals(value)) {
          value = entry.getKey();
        }
      }
    }
    return value;
  }

  private List<String> extractSegmentsData(String segmentQuery, List<Segment> segments) {
    List<String> componentsValues = new ArrayList<>();

    String segmentTag = segmentQuery.substring(0, SEGMENT_TAG_LENGTH);
    String qualifierValue = null;
    List<String> dataElementsFilterValues;
    String dataElementSeparator = determineDataElementSeparator(segmentQuery);
    int targetDataElementIndex = StringUtils.countMatches(segmentQuery, dataElementSeparator) - 1;
    int targetComponentIndex = Integer.parseInt(StringUtils.substringBetween(segmentQuery, "[", "]")) - 1;

    if (isContainsQualifier(segmentQuery)) {
      qualifierValue = StringUtils.substringBetween(segmentQuery, QUALIFIER_SIGN, "[");
      dataElementsFilterValues = Arrays.asList(StringUtils.split(segmentQuery.substring(4, segmentQuery.indexOf(QUALIFIER_SIGN)), dataElementSeparator));
    } else {
      dataElementsFilterValues = Arrays.asList(StringUtils.split(segmentQuery.substring(4, segmentQuery.indexOf('[')), dataElementSeparator));
    }

    for (Segment segment : segments) {
      if (segment.getTag().equals(segmentTag) && segment.getDataElements().size() >= dataElementsFilterValues.size()) {
        DataElement lastDataElement = Iterables.getLast(segment.getDataElements());
        String segmentValueQualifier = Iterables.getLast(lastDataElement.getComponents()).getData();

        if (qualifierValue == null || qualifierValue.equals(segmentValueQualifier)) {
          List<String> currentDataElementsValues = segment.getDataElements().stream()
            .limit(dataElementsFilterValues.size())
            .map(dataElement -> dataElement.getComponents().get(0).getData())
            .collect(Collectors.toList());

          if (dataElementsFilterValues.equals(currentDataElementsValues)) {
            DataElement targetDataElement = segment.getDataElements().get(targetDataElementIndex);
            componentsValues.add(targetDataElement.getComponents().get(targetComponentIndex).getData());  // todo: implement for range
          }
        }
      }
    }

    return componentsValues;
  }

  private String determineDataElementSeparator(String segmentQuery) {
    return segmentQuery.substring(3, 4);
  }

  private boolean isContainsQualifier(String segmentQuery) {
    return StringUtils.substringAfterLast(segmentQuery, "+").contains(QUALIFIER_SIGN);
  }

}
