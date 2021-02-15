package org.folio.processing.mapping.mapper.reader.record.edifact;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;

public class EdifactReaderFactory implements ReaderFactory {

  @Override
  public Reader createReader() {
    return new EdifactRecordReader(EDIFACT_INVOICE);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return EDIFACT_INVOICE.equals(entityType);
  }
}
