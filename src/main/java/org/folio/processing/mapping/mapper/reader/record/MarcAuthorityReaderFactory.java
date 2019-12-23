package org.folio.processing.mapping.mapper.reader.record;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;

import static org.folio.processing.mapping.model.MappingProfile.EntityType;
import static org.folio.processing.mapping.model.MappingProfile.EntityType.MARC_AUTHORITY;

public class MarcAuthorityReaderFactory implements ReaderFactory {
  @Override
  public Reader createReader() {
    return new MarcRecordReader(MARC_AUTHORITY);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return MARC_AUTHORITY.equals(entityType);
  }
}
