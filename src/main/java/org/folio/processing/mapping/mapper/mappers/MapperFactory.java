package org.folio.processing.mapping.mapper.mappers;

import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.mapper.Mapper;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;

/**
 * Factory for creating specific mapper based on condition inside payload
 */
public interface MapperFactory {

  /**
   *  Create a new mapper for the specific type
   * @param reader - reader
   * @param writer - writer
   * @return specific Mapper
   */
  Mapper createMapper(Reader reader, Writer writer);

  /**
   * Check if current mapper suits for current payload
   * @param eventPayload - current DataImportEventPayload
   * @return true if current mapper suits for this payload
   */
  boolean isEligiblePayload(DataImportEventPayload eventPayload);
}
