package org.folio.processing.mapping.mapper.mappers;

import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.mapper.Mapper;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;

public interface MapperFactory {

  Mapper createMapper(Reader reader, Writer writer);

  boolean isEligiblePayload(DataImportEventPayload eventPayload);
}
