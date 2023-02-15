package org.folio.processing.mapping.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.mapper.writer.AbstractWriter;
import org.folio.processing.value.BooleanValue;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MapValue;
import org.folio.processing.value.RepeatableFieldValue;
import org.folio.processing.value.StringValue;

import java.io.IOException;
import java.util.HashMap;

import static org.folio.rest.jaxrs.model.EntityType.ORDER;

public class TestOrderWriter extends AbstractWriter {
  private TestOrder order;

  TestOrderWriter() {
  }

  @Override
  public void initialize(DataImportEventPayload eventPayload) throws IOException {
    if (eventPayload.getContext().containsKey(ORDER.value())) {
      this.order = new ObjectMapper().readValue(eventPayload.getContext().get(ORDER.value()), TestOrder.class);
    } else {
      throw new IllegalArgumentException("Can not initialize OrderWriter, no order found in context");
    }
  }

  @Override
  public DataImportEventPayload getResult(DataImportEventPayload eventPayload) throws JsonProcessingException {
    HashMap<String, String> context = eventPayload.getContext();
    context.put(ORDER.value(), new ObjectMapper().writeValueAsString(this.order));
    eventPayload.setContext(context);
    return eventPayload;
  }

  @Override
  protected void writeStringValue(String fieldPath, StringValue value) {
    if (fieldPath.equals("order.po.vendor")) {
      this.order.setVendor(value.getValue());
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
