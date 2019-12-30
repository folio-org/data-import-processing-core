package org.folio.processing.mapping.mapper.reader.record;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;

import static org.folio.processing.mapping.model.MappingProfile.EntityType;
import static org.folio.processing.mapping.model.MappingProfile.EntityType.MARC_HOLDINGS;

/**
 * Factory to create reader of marc holdings records
 */
public class MarcHoldingsReaderFactory implements ReaderFactory {
  @Override
  public Reader createReader() {
    return new MarcRecordReader(MARC_HOLDINGS);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return MARC_HOLDINGS.equals(entityType);
  }
}
