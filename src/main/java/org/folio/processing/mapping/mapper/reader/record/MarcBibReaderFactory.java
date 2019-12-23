package org.folio.processing.mapping.mapper.reader.record;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;

import static org.folio.processing.mapping.model.MappingProfile.EntityType;
import static org.folio.processing.mapping.model.MappingProfile.EntityType.MARC_BIBLIOGRAPHIC;

/**
 * Factory to create reader of marc bibliographic records
 */
public class MarcBibReaderFactory implements ReaderFactory {
  @Override
  public Reader createReader() {
    return new MarcRecordReader(MARC_BIBLIOGRAPHIC);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return MARC_BIBLIOGRAPHIC.equals(entityType);
  }
}
