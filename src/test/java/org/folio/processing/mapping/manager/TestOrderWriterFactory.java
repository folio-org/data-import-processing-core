package org.folio.processing.mapping.manager;

import static org.folio.rest.jaxrs.model.EntityType.ORDER;

import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.rest.jaxrs.model.EntityType;

public class TestOrderWriterFactory implements WriterFactory {

  TestOrderWriterFactory() {
  }

  @Override
  public Writer createWriter() {
    return new TestOrderWriter();
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return ORDER == entityType;
  }
}
