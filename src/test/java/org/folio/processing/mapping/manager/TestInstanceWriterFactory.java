package org.folio.processing.mapping.manager;

import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;

public class TestInstanceWriterFactory implements WriterFactory {

  TestInstanceWriterFactory() {
  }

  @Override
  public Writer createWriter() {
    return new TestInstanceWriter();
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return INSTANCE == entityType;
  }
}
