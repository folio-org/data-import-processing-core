package org.folio.processing.mapping.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.DataImportEventPayload;
import org.folio.processing.value.BooleanValue;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MapValue;
import org.folio.processing.value.RepeatableFieldValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.mapping.mapper.writer.AbstractWriter;

import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;


import java.io.IOException;
import java.util.HashMap;

public class TestInstanceWriter extends AbstractWriter {
  private TestInstance instance;

  TestInstanceWriter() {
  }

  @Override
  public void initialize(DataImportEventPayload eventPayload) throws IOException {
    if (eventPayload.getContext().containsKey(INSTANCE.value())) {
      this.instance = new ObjectMapper().readValue(eventPayload.getContext().get(INSTANCE.value()), TestInstance.class);
    } else {
      throw new IllegalArgumentException("Can not initialize InstanceWriter, no instance found in context");
    }
  }

  @Override
  public DataImportEventPayload getResult(DataImportEventPayload eventPayload) throws JsonProcessingException {
    HashMap<String, String> context = eventPayload.getContext();
    context.put(INSTANCE.value(), new ObjectMapper().writeValueAsString(this.instance));
    eventPayload.setContext(context);
    return eventPayload;
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

  @Override
  protected void writeObjectValue(String fieldPath, MapValue value) {

  }

  @Override
  protected void writeRepeatableValue(String fieldPath, RepeatableFieldValue value) {

  }

  @Override
  protected void writeBooleanValue(String fieldPath, BooleanValue value) {

  }
}
