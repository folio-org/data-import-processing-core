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

  /**
   *
   * Template method for mapping.
   * @param profile - current Mapping Profile
   * @param eventPayload - current eventPayload
   * @param mappingContext - current Context
   * @return - DataImportEventPayload with mapped entities inside
   */
  DataImportEventPayload map(MappingProfile profile, DataImportEventPayload eventPayload,
                             MappingContext mappingContext);

  /**
   * Initialization reader and writer
   * @param reader    -     Reader to read values from given event payload
   * @param writer     -    Writer to write values to given event payload
   * @param eventPayload - current eventPayload
   * @param mappingContext - current Context
   * @throws IOException if a low-level I/O problem occurs (JSON serialization)
   */
  default void initializeReaderAndWriter(DataImportEventPayload eventPayload, Reader reader, Writer writer, MappingContext mappingContext) throws IOException {
    reader.initialize(eventPayload, mappingContext);
    writer.initialize(eventPayload);
  }

  /**
   * Check if MappingProfile is valid
   * @param profile - current Mapping Profile
   * @return true if MappingProfile is valid otherwise - false.
   */
  default boolean ifProfileIsInvalid( MappingProfile profile) {
    return profile.getMappingDetails() == null
      || profile.getMappingDetails().getMappingFields() == null
      || profile.getMappingDetails().getMappingFields().isEmpty();
  }
}
