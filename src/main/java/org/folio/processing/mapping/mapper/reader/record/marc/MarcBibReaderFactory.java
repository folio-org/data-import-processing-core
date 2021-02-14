package org.folio.processing.mapping.mapper.reader.record;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;

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
