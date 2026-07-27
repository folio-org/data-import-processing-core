package org.folio.processing.mapping.manager;

import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;

import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.MappingRule;

public class TestMarcBibliographicReader implements Reader {

  TestMarcBibliographicReader() {
  }

  @Override
  public void initialize(DataImportEventPayload eventPayload, MappingContext mappingContext) {
    if (!eventPayload.getContext().containsKey(MARC_BIBLIOGRAPHIC.value())) {
      throw new IllegalArgumentException("Can not initialize MarcBibliographicReader, no record found in context");
    }
  }

  @Override
  public Value read(MappingRule ruleExpression) {
    return StringValue.of("test index title");
  }
}
