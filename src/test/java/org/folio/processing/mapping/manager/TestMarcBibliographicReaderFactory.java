package org.folio.processing.mapping.manager;

import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.rest.jaxrs.model.EntityType;

public class TestMarcBibliographicReaderFactory implements ReaderFactory {

  TestMarcBibliographicReaderFactory() {
  }

  @Override
  public Reader createReader() {
    return new TestMarcBibliographicReader();
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return MARC_BIBLIOGRAPHIC == entityType;
  }
}
