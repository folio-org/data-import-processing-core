package org.folio.processing.mapping.mapper.reader.record;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;

/**
 * Factory to create reader of marc authority records
 */
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
