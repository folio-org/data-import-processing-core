package org.folio.processing.mapping.manager;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.processing.mapping.model.MappingProfile;

import static org.folio.processing.mapping.model.MappingProfile.EntityType.MARC_BIBLIOGRAPHIC;

public class TestMarcBibliographicReaderFactory implements ReaderFactory {

  TestMarcBibliographicReaderFactory() {
  }

  @Override
  public Reader createReader() {
    return new TestMarcBibliographicReader();
  }

  @Override
  public boolean isEligibleForEntityType(MappingProfile.EntityType entityType) {
    return MARC_BIBLIOGRAPHIC == entityType;
  }
}
