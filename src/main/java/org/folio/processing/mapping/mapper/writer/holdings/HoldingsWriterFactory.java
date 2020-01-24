package org.folio.processing.mapping.mapper.writer.holdings;

import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.processing.mapping.model.MappingProfile;

import static org.folio.processing.mapping.model.MappingProfile.EntityType.HOLDINGS;

public class HoldingsWriterFactory implements WriterFactory {

  @Override
  public Writer createWriter() {
    return new JsonBasedWriter(HOLDINGS);
  }

  @Override
  public boolean isEligibleForEntityType(MappingProfile.EntityType entityType) {
    return HOLDINGS == entityType;
  }
}
