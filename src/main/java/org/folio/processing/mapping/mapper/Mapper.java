package org.folio.processing.mapping.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;

/**
 * The central component for reading data from source and writing data to target.
 *
 * @see Reader
 * @see Writer
 * @see Value
 */
public interface Mapper {

  boolean isEligibleForEntityType(DataImportEventPayload eventPayload);


  DataImportEventPayload map(MappingProfile profile, DataImportEventPayload eventPayload,
                             MappingContext mappingContext);

  default void initializeReaderAndWriter(DataImportEventPayload eventPayload, Reader reader, Writer writer, MappingContext mappingContext) throws IOException {
    reader.initialize(eventPayload, mappingContext);
    writer.initialize(eventPayload);
  }

  default boolean ifProfileIsInvalid(DataImportEventPayload eventPayload, MappingProfile profile) {
    return profile.getMappingDetails() == null
      || profile.getMappingDetails().getMappingFields() == null
      || profile.getMappingDetails().getMappingFields().isEmpty();
  }
}
