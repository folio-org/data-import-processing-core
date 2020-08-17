package org.folio.processing.mapping.manager;

import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordWriter;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;

public class TestMarcRecordWriterFactory implements WriterFactory {

  @Override
  public Writer createWriter() {
    return new MarcRecordWriter(MARC_BIBLIOGRAPHIC);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return MARC_BIBLIOGRAPHIC == entityType;
  }
}
