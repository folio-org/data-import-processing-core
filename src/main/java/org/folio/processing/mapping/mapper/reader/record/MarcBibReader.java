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

  private JsonNode fields = null;

  @Override
  public void initialize(EventContext eventContext) throws IOException {
    if (eventContext.getObjects().containsKey(MARC_BIBLIOGRAPHIC.value())) {
      String stringRecord = eventContext.getObjects().get(MARC_BIBLIOGRAPHIC.value());
      this.fields = new ObjectMapper().readTree(stringRecord).at("/fields");
    } else {
      throw new IllegalArgumentException("Can not initialize MarcBibliographicReader, no record found in context");
    }
  }

  @Override
  public Value read(Rule rule) {
    this.fields.findValue("002");
    return new StringValue("");
  }
}
