package org.folio.processing.mapping.mapper;

import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.value.Value;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.model.MappingProfile;
import org.folio.processing.mapping.model.Rule;

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
  default DataImportEventPayload map(Reader reader, Writer writer, DataImportEventPayload eventPayload) throws IOException {
    ProfileSnapshotWrapper mappingProfileWrapper = eventPayload.getCurrentNode();
    MappingProfile mappingProfile = (MappingProfile) mappingProfileWrapper.getContent();
    reader.initialize(eventPayload);
    writer.initialize(eventPayload);
    List<Rule> mappingRules = mappingProfile.getMappingRules();
    for (Rule rule : mappingRules) {
      Value value = reader.read(rule.getRuleExpression());
      writer.write(rule.getFieldPath(), value);
    }
    return writer.getResult(eventPayload);
  }
}
