package org.folio.processing.mapping.mapper.reader.record;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.value.MissingValue;
import org.folio.processing.mapping.mapper.value.StringValue;
import org.folio.processing.mapping.mapper.value.Value;
import org.folio.processing.mapping.model.MappingProfile.EntityType;

import java.io.IOException;

@SuppressWarnings("all")
public class MarcRecordReader implements Reader {
  private static final String MARC_FIELDS_POINTER = "/fields";
  private EntityType entityType;
  private JsonNode fieldsNode = null;

  MarcRecordReader(EntityType entityType) {
    this.entityType = entityType;
  }

  @Override
  public void initialize(EventContext eventContext) throws IOException {
    if (eventContext.getObjects().containsKey(entityType.value())) {
      String stringRecord = eventContext.getObjects().get(entityType.value());
      this.fieldsNode = new ObjectMapper().readTree(stringRecord).at(MARC_FIELDS_POINTER);
    } else {
      throw new IllegalArgumentException("Can not initialize MarcRecordReader, no suitable entity type found in context");
    }
  }

  @Override
  public Value read(String ruleExpression) {
    String tag = getTag(ruleExpression);
    String condition = getCondition(ruleExpression);
    return readByCondition(tag, condition);
  }

  private String getTag(String ruleExpression) {
    // Stub implementation
    return ruleExpression;
  }

  private String getCondition(String ruleExpression) {
    // Stub implementation
    return StringUtils.EMPTY;
  }

  private Value readByCondition(String tag, String condition) {
    // Stub implementation
    Value result = MissingValue.getInstance();
    JsonNode fieldNode = this.fieldsNode.findValue(tag);
    if (fieldNode != null && fieldNode.isTextual()) {
      result = StringValue.of(fieldNode.textValue());
    }
    return result;
  }
}
