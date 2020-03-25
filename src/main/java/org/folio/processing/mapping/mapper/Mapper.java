package org.folio.processing.mapping.mapper;

import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.MappingRule;

import java.io.IOException;
import java.util.List;

/**
 * The central component for reading data from source and writing data to target.
 *
 * @see Reader
 * @see Writer
 * @see Value
 */
public interface Mapper {

  /**
   * Template method for mapping.
   *
   * @param reader       Reader to read values from given event payload
   * @param writer       Writer to write values to given event payload
   * @param eventPayload event payload
   * @return event payload
   * @throws IOException if a low-level I/O problem occurs (JSON serialization)
   */
  default DataImportEventPayload map(Reader reader, Writer writer, MappingProfile profile, DataImportEventPayload eventPayload) throws IOException {
    reader.initialize(eventPayload);
    writer.initialize(eventPayload);
    List<MappingRule> mappingRules = profile.getMappingDetails().getMappingFields();
    for (MappingRule rule : mappingRules) {
      if (Boolean.valueOf(rule.getEnabled())){
        Value value = reader.read(rule);
        writer.write(rule.getPath(), value);
      }
    }
    return writer.getResult(eventPayload);
  }
}
