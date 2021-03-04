package org.folio.processing.mapping.util;

import com.google.common.collect.Iterables;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.Component;
import org.folio.DataElement;
import org.folio.EdifactParsedContent;
import org.folio.ParsedRecord;
import org.folio.Segment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;

public class EdifactParsedRecordUtil {

  private static final Logger LOGGER = LogManager.getLogger(EdifactParsedRecordUtil.class);

  private static final String PARSED_RECORD_HAS_NO_DATA_MSG = "Failed to retrieve segments data - parsed record does not contain EDIFACT data";
  private static final String INVALID_MAPPING_EXPRESSION_MSG = "The specified segment mapping expression '%s' is invalid";
  private static final String INVALID_DATA_RANGE_MSG = "The specified components data range is invalid: from '%s' to '%s'. From index must be less than or equal to the end index.";
  private static final Pattern SEGMENT_QUERY_PATTERN = Pattern.compile("[A-Z]{3}((\\+|<)\\w*)(\\2*\\w*)*(\\?\\w+)?\\[[1-9](-[1-9])?\\]");
  private static final String INVOICE_LINE_ITEM_TAG = "LIN";
  private static final String INVOICE_SUMMARY_TAG = "UNS";
  private static final String RANGE_DELIMITER = "-";
  private static final String QUALIFIER_SIGN = "?";
  private static final int SEGMENT_TAG_LENGTH = 3;

  /**
   * Extracts data from the invoice lines segments specified in the {@code segmentMappingExpression}.
   *
   * @param parsedRecord             parsed record with EDIFACT parsed content
   * @param segmentMappingExpression mapping expression with segment to extract data from
   * @return map with segments data and corresponding invoice lines numbers
   * @throws IllegalArgumentException if {@code parsedRecord} has no EDIFACT parsed content
   *   and when invalid segment mapping expression is specified
   */
  public static Map<Integer, String> getInvoiceLinesSegmentsValues(ParsedRecord parsedRecord, String segmentMappingExpression) {
    if (parsedRecord == null || parsedRecord.getContent() == null) {
      LOGGER.error(PARSED_RECORD_HAS_NO_DATA_MSG);
      throw new IllegalArgumentException(PARSED_RECORD_HAS_NO_DATA_MSG);
    } else if (!SEGMENT_QUERY_PATTERN.matcher(segmentMappingExpression).matches()) {
      String msg = format(INVALID_MAPPING_EXPRESSION_MSG, segmentMappingExpression);
      LOGGER.error(msg);
      throw new IllegalArgumentException(msg);
    }

    EdifactParsedContent parsedContent = Json.decodeValue(parsedRecord.getContent().toString(), EdifactParsedContent.class);
    List<List<Segment>> invoiceLinesSegmentGroups = getInvoiceLinesSegments(parsedContent);
    HashMap<Integer, String> invLineNoToSegmentValue = new HashMap<>();

    for (int i = 0; i < invoiceLinesSegmentGroups.size(); i++) {
      List<Segment> invoiceLineSegments = invoiceLinesSegmentGroups.get(i);
      List<String> segmentsData = extractSegmentsData(segmentMappingExpression, invoiceLineSegments);

      if (!segmentsData.isEmpty()) {
        String readValue = String.join(EMPTY, segmentsData);
        invLineNoToSegmentValue.put(i + 1, readValue);
      }
    }
    return invLineNoToSegmentValue;
  }

  private static List<List<Segment>> getInvoiceLinesSegments(EdifactParsedContent edifactParsedContent) {
    List<List<Segment>> invoiceLinesSegments = new ArrayList<>();
    int invoiceLineStart = 0;
    int invoiceLineEndExclusive;
    List<Segment> segments = edifactParsedContent.getSegments();

    for (int i = 0; i < segments.size(); i++) {
      Segment segment = segments.get(i);
      if (INVOICE_LINE_ITEM_TAG.equals(segment.getTag())) {
        if (invoiceLineStart != 0) {
          invoiceLineEndExclusive = i;
          invoiceLinesSegments.add(segments.subList(invoiceLineStart, invoiceLineEndExclusive));
        }
        invoiceLineStart = i;
      } else if (INVOICE_SUMMARY_TAG.equals(segment.getTag())) {
        invoiceLineEndExclusive = i;
        invoiceLinesSegments.add(segments.subList(invoiceLineStart, invoiceLineEndExclusive));
        break;
      }
    }
    return invoiceLinesSegments;
  }

  private static List<String> extractSegmentsData(String segmentQuery, List<Segment> segments) {
    List<String> componentsValues = new ArrayList<>();

    String segmentTag = segmentQuery.substring(0, SEGMENT_TAG_LENGTH);
    String qualifierValue = null;
    List<String> dataElementsFilterValues;
    String dataElementSeparator = determineDataElementSeparator(segmentQuery);
    int targetDataElementIndex = StringUtils.countMatches(segmentQuery, dataElementSeparator) - 1;

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
            Pair<Integer, Integer> componentsRange = extractComponentPositionsRange(segmentQuery);
            componentsValues.add(getComponentsData(targetDataElement, componentsRange));
          }
        }
      }
    }
    return componentsValues;
  }

  private static String determineDataElementSeparator(String segmentQuery) {
    return segmentQuery.substring(3, 4);
  }

  private static boolean isContainsQualifier(String segmentQuery) {
    String dataElementSeparator = determineDataElementSeparator(segmentQuery);
    return StringUtils.substringAfterLast(segmentQuery, dataElementSeparator).contains(QUALIFIER_SIGN);
  }

  private static Pair<Integer, Integer> extractComponentPositionsRange(String segmentQuery) {
    int fromIndex;
    int toIndex;
    String positionsStatement = StringUtils.substringBetween(segmentQuery, "[", "]");
    if (positionsStatement.contains(RANGE_DELIMITER)) {
      fromIndex = Integer.parseInt(StringUtils.substringBefore(positionsStatement, RANGE_DELIMITER));
      toIndex = Integer.parseInt(StringUtils.substringAfter(positionsStatement, RANGE_DELIMITER));
    } else {
      fromIndex = Integer.parseInt(positionsStatement);
      toIndex = fromIndex;
    }
    return Pair.of(fromIndex, toIndex);
  }

  private static String getComponentsData(DataElement dataElement, Pair<Integer, Integer> componentsRange) {
    int fromComponent = componentsRange.getLeft();
    int toComponent = componentsRange.getRight();

    if (fromComponent > toComponent) {
      throw new IllegalArgumentException(String.format(INVALID_DATA_RANGE_MSG, fromComponent, toComponent));
    }

    int lastComponentIndex = Math.min(toComponent, dataElement.getComponents().size());
    return dataElement.getComponents().subList(fromComponent - 1, lastComponentIndex)
      .stream()
      .map(Component::getData)
      .collect(Collectors.joining());
  }
}
