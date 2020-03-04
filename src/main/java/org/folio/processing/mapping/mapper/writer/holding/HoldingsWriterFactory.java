package org.folio.processing.mapping.mapper.writer.holding;

import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;

public class HoldingsWriterFactory implements WriterFactory {

  @Override
  public Writer createWriter() {
    return new JsonBasedWriter(HOLDINGS);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return HOLDINGS == entityType;
  }
}