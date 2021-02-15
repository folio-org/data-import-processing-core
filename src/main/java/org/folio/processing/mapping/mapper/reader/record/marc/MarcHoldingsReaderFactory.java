package org.folio.processing.mapping.mapper.reader.record.marc;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.rest.jaxrs.model.EntityType.MARC_HOLDINGS;

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
