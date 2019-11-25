package org.folio.processing.mapping.writer;

import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.model.MappingProfile;

import static org.folio.processing.mapping.model.MappingProfile.EntityType.INSTANCE;

public class TestInstanceWriterFactory implements WriterFactory {
  @Override
  public Writer createWriter() {
    return new TestInstanceWriter();
  }

  @Override
  public boolean isEligibleForEntityType(MappingProfile.EntityType entityType) {
    return INSTANCE == entityType;
  }
}
