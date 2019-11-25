package org.folio.processing.mapping.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.mapper.value.ListValue;
import org.folio.processing.mapping.mapper.value.StringValue;
import org.folio.processing.mapping.mapper.writer.AbstractWriter;

import java.io.IOException;

import static org.folio.processing.mapping.model.MappingProfile.EntityType.INSTANCE;

public class TestInstanceWriter extends AbstractWriter {
  private Instance instance;

  @Override
  public void initialize(EventContext eventContext) throws IOException {
    if (eventContext.getObjects().containsKey(INSTANCE.value())) {
      this.instance = new ObjectMapper().readValue(eventContext.getObjects().get(INSTANCE.value()), Instance.class);
    } else {
      throw new IllegalArgumentException("Can not initialize InstanceWriter, no instance found in context");
    }
  }

  @Override
  public EventContext getResult(EventContext eventContext) throws JsonProcessingException {
    eventContext.putObject(INSTANCE.value(), new ObjectMapper().writeValueAsString(this.instance));
    return eventContext;
  }

  @Override
  protected void writeStringValue(String fieldPath, StringValue value) {
    if (fieldPath.equals("indexTitle")) {
      this.instance.setIndexTitle("test index title");
    }
  }

  @Override
  protected void writeListValue(String fieldPath, ListValue value) {

  }
}
