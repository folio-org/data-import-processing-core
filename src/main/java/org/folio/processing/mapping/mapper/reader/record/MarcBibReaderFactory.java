package org.folio.processing.mapping.mapper.reader.record;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;

import static org.folio.processing.mapping.model.MappingProfile.EntityType;
import static org.folio.processing.mapping.model.MappingProfile.EntityType.MARC_BIBLIOGRAPHIC;

public class MarcBibReaderFactory implements ReaderFactory {
  @Override
  public Reader createReader() {
    return new MarcBibReader();
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return MARC_BIBLIOGRAPHIC.equals(entityType);
  }
}
