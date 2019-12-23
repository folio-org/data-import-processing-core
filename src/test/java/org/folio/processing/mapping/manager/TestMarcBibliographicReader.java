package org.folio.processing.mapping.manager;

import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.value.StringValue;
import org.folio.processing.mapping.mapper.value.Value;

import static org.folio.processing.mapping.model.MappingProfile.EntityType.MARC_BIBLIOGRAPHIC;

public class TestMarcBibliographicReader implements Reader {

  private String marcBibliographicRecord;

  TestMarcBibliographicReader() {
  }

  @Override
  public void initialize(EventContext eventContext) {
    if (eventContext.getObjects().containsKey(MARC_BIBLIOGRAPHIC.value())) {
      this.marcBibliographicRecord = eventContext.getObjects().get(MARC_BIBLIOGRAPHIC.value());
    } else {
      throw new IllegalArgumentException("Can not initialize MarcBibliographicReader, no record found in context");
    }
  }

  @Override
  public Value read(String ruleExpression) {
    return StringValue.of("test index title");
  }
}