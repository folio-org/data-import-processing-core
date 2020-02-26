package org.folio.processing.mapping.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.DataImportEventPayload;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MapValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.mapping.mapper.writer.AbstractWriter;

import java.io.IOException;
import java.util.HashMap;

import static org.folio.processing.mapping.model.MappingProfile.EntityType.INSTANCE;

public class TestInstanceWriter extends AbstractWriter {
  private TestInstance instance;

  TestInstanceWriter() {
  }

  @Override
  public void initialize(DataImportEventPayload eventContext) throws IOException {
    if (eventContext.getContext().containsKey(INSTANCE.value())) {
      this.instance = new ObjectMapper().readValue(eventContext.getContext().get(INSTANCE.value()), TestInstance.class);
    } else {
      throw new IllegalArgumentException("Can not initialize InstanceWriter, no instance found in context");
    }
  }

  @Override
  public DataImportEventPayload getResult(DataImportEventPayload eventContext) throws JsonProcessingException {
    HashMap<String, String> context = eventContext.getContext();
    context.put(INSTANCE.value(), new ObjectMapper().writeValueAsString(this.instance));
    eventContext.setContext(context);
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

  @Override
  protected void writeObjectValue(String fieldPath, MapValue value) {

  }
}
