package org.folio.processing.mapping.mapper.reader.record.edifact;

import com.google.common.collect.Iterables;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataElement;
import org.folio.DataImportEventPayload;
import org.folio.EdifactParsedContent;
import org.folio.Record;
import org.folio.Segment;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class EdifactRecordReader implements Reader {

  public static final Pattern DATA_ELEMENT_SEPARATOR_PATTERN = Pattern.compile("[+|<]");
  public static final String QUALIFIER_MARK = "Q";
  public static final int SEGMENT_TAG_LENGTH = 3;

  private EntityType entityType;
  private EdifactParsedContent edifactParsedContent;

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
      } else {
        throw new IllegalArgumentException("Can not initialize EdifactRecordReader, event payload has no EDIFACT parsed content");
      }
    }
  }

  @Override
  public Value read(MappingRule mappingRule) {
    if (mappingRule.getSubfields().isEmpty() && isNotEmpty(mappingRule.getValue())) {
      String mappingExpression = mappingRule.getValue();

      String segmentQuery = mappingExpression;
      List<String> segmentsData = extractSegmentsData(segmentQuery);
      return StringValue.of(String.join(EMPTY, segmentsData));
    }

    return MissingValue.getInstance();
  }

  private List<String> extractSegmentsData(String segmentQuery) {
    List<String> componentsValues = new ArrayList<>();

    String segmentTag = segmentQuery.substring(0, SEGMENT_TAG_LENGTH);
    String qualifierValue = null;
    List<String> dataElementsFilterValues;
    int targetDataElementIndex = StringUtils.countMatches(segmentQuery, '+') - 1;
    int targetComponentIndex = Integer.parseInt(StringUtils.substringBetween(segmentQuery, "[", "]")) - 1;

    if (isContainsQualifier(segmentQuery)) {
      qualifierValue = StringUtils.substringBetween(segmentQuery, QUALIFIER_MARK, "[");
      dataElementsFilterValues = Arrays.asList(segmentQuery.substring(4, segmentQuery.indexOf(QUALIFIER_MARK)).split("\\+"));
    } else {
      dataElementsFilterValues = Arrays.asList(segmentQuery.substring(4, segmentQuery.indexOf('[')).split("\\+"));
    }

    for (Segment segment : edifactParsedContent.getSegments()) {
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

  private boolean isContainsQualifier(String segmentQuery) {
    return StringUtils.substringAfterLast(segmentQuery, "+").contains(QUALIFIER_MARK);
  }

}
