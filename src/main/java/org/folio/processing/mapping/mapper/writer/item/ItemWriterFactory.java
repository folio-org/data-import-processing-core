package org.folio.processing.mapping.mapper.writer.item;

import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.rest.jaxrs.model.EntityType.ITEM;

public class ItemWriterFactory implements WriterFactory {

  @Override
  public Writer createWriter() {
    return new JsonBasedWriter(ITEM);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return ITEM == entityType;
  }
}
