package org.folio.processing.mapping.manager;

import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;

import static org.folio.processing.mapping.model.MappingProfile.EntityType.MARC_BIBLIOGRAPHIC;

public class TestMarcBibliographicReader implements Reader {

  private String marcBibliographicRecord;

  TestMarcBibliographicReader() {
  }

  @Override
  public void initialize(DataImportEventPayload eventContext) {
    if (eventContext.getContext().containsKey(MARC_BIBLIOGRAPHIC.value())) {
      this.marcBibliographicRecord = eventContext.getContext().get(MARC_BIBLIOGRAPHIC.value());
    } else {
      throw new IllegalArgumentException("Can not initialize MarcBibliographicReader, no record found in context");
    }
  }

  @Override
  public Value read(String ruleExpression) {
    return StringValue.of("test index title");
  }
}
