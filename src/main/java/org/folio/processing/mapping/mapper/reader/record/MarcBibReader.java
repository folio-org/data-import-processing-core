package org.folio.processing.mapping.mapper.reader.record;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.value.StringValue;
import org.folio.processing.mapping.mapper.value.Value;
import org.folio.processing.mapping.model.Rule;

import java.io.IOException;

import static org.folio.processing.mapping.model.MappingProfile.EntityType.MARC_BIBLIOGRAPHIC;

public class MarcBibReader implements Reader {

  private JsonNode jsonRecord = null;

  @Override
  public void initialize(EventContext eventContext) throws IOException {
    if (eventContext.getObjects().containsKey(MARC_BIBLIOGRAPHIC)) {
      String stringRecord = eventContext.getObjects().get(MARC_BIBLIOGRAPHIC);
      this.jsonRecord = new ObjectMapper().readTree(stringRecord);
    } else {
      throw new IllegalArgumentException("Can not initialize MarcBibliographicReader, no record found in context");
    }
  }

  @Override
  public Value read(Rule rule) {
    JsonNode recordField = this.jsonRecord.get(rule.getFieldPath());
    recordField.getNodeType().
    return new StringValue("");
  }
}
