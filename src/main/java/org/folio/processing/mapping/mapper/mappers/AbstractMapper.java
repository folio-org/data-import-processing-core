package org.folio.processing.mapping.mapper.mappers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.exceptions.MappingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.Mapper;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.MappingRule;

import java.io.IOException;
import java.util.List;

public class AbstractMapper implements Mapper {
  private static final Logger LOGGER = LogManager.getLogger(MappingManager.class);

  private Reader reader;
  private Writer writer;

  public AbstractMapper(Reader reader, Writer writer) {
    this.reader = reader;
    this.writer = writer;
  }

  @Override
  public boolean isEligibleForEntityType(DataImportEventPayload eventPayload) {
    return false;
  }

  @Override
  public DataImportEventPayload map(MappingProfile profile, DataImportEventPayload eventPayload, MappingContext mappingContext) {
    try {
      initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
      if (ifProfileIsInvalid(eventPayload, profile)) {
        return eventPayload;
      }
      List<MappingRule> mappingRules = profile.getMappingDetails().getMappingFields();
      for (MappingRule rule : mappingRules) {
        if (Boolean.parseBoolean(rule.getEnabled())) {
          Value value = reader.read(rule);
          writer.write(rule.getPath(), value);
        }
      }
      return writer.getResult(eventPayload);
    } catch (IOException e) {
      LOGGER.warn("map:: Failed to perform Abstract mapping", e);
      throw new MappingException(e);
    }
  }
}
