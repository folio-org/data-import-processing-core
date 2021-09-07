package org.folio.processing.mapping.generateHoldings;


import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.rest.jaxrs.model.EntityType;

public class HoldingsWriterFactory implements WriterFactory {

  @Override
  public Writer createWriter() {
    return new JsonBasedWriter(EntityType.HOLDINGS);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return EntityType.HOLDINGS == entityType;
  }
}
