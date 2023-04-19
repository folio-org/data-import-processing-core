package org.folio.processing.mapping.mapper;

import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.value.Value;

import java.io.IOException;

/**
 * The central component for reading data from source and writing data to target.
 *
 * @see Reader
 * @see Writer
 * @see Value
 */
public interface Mapper {


  DataImportEventPayload map(MappingProfile profile, DataImportEventPayload eventPayload,
                             MappingContext mappingContext);

  default void initializeReaderAndWriter(DataImportEventPayload eventPayload, Reader reader, Writer writer, MappingContext mappingContext) throws IOException {
    reader.initialize(eventPayload, mappingContext);
    writer.initialize(eventPayload);
  }

  default boolean ifProfileIsInvalid( MappingProfile profile) {
    return profile.getMappingDetails() == null
      || profile.getMappingDetails().getMappingFields() == null
      || profile.getMappingDetails().getMappingFields().isEmpty();
  }
}
