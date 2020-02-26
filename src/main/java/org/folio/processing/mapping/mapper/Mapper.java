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
   * @param reader       Reader to read values from given event context
   * @param writer       Writer to write values to given event context
   * @param eventContext event context
   * @return event context
   * @throws IOException if a low-level I/O problem occurs (JSON serialization)
   */
  default DataImportEventPayload map(Reader reader, Writer writer, DataImportEventPayload eventContext) throws IOException {
    ProfileSnapshotWrapper mappingProfileWrapper = eventContext.getCurrentNode();
    MappingProfile mappingProfile = (MappingProfile) mappingProfileWrapper.getContent();
    reader.initialize(eventContext);
    writer.initialize(eventContext);
    List<Rule> mappingRules = mappingProfile.getMappingRules();
    for (Rule rule : mappingRules) {
      Value value = reader.read(rule.getRuleExpression());
      writer.write(rule.getFieldPath(), value);
    }
    return writer.getResult(eventContext);
  }
}
