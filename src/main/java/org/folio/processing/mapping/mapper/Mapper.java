package org.folio.processing.mapping.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.MappingRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.folio.processing.mapping.mapper.reader.record.marc.MarcRecordReader.IF_DUPLICATES_NEEDED;

/**
 * The central component for reading data from source and writing data to target.
 *
 * @see Reader
 * @see Writer
 * @see Value
 */
public interface Mapper {
  String MARC_BIBLIOGRAPHIC = "MARC_BIBLIOGRAPHIC";
  String CONTENT = "content";
  String FIELDS = "fields";
  String PARSED_RECORD = "parsedRecord";

  /**
   * Template method for mapping.
   *
   * @param profile        - current Mapping Profile
   * @param eventPayload   - current eventPayload
   * @param mappingContext - current Context
   * @return - DataImportEventPayload with mapped entities inside
   */
  DataImportEventPayload map(MappingProfile profile, DataImportEventPayload eventPayload, MappingContext mappingContext);

  /**
   * Initialization reader and writer
   *
   * @param reader         -     Reader to read values from given event payload
   * @param writer         -    Writer to write values to given event payload
   * @param eventPayload   - current eventPayload
   * @param mappingContext - current Context
   * @throws IOException if a low-level I/O problem occurs (JSON serialization)
   */
  default void initializeReaderAndWriter(DataImportEventPayload eventPayload, Reader reader, Writer writer, MappingContext mappingContext) throws IOException {
    reader.initialize(eventPayload, mappingContext);
    writer.initialize(eventPayload);
  }

  /**
   * Check if MappingProfile is valid
   *
   * @param profile - current Mapping Profile
   * @return true if MappingProfile is valid otherwise - false.
   */
  default boolean ifProfileIsInvalid(MappingProfile profile) {
    return profile.getMappingDetails() == null || profile.getMappingDetails().getMappingFields() == null || profile.getMappingDetails().getMappingFields().isEmpty();
  }

  default JsonArray mapMultipleEntities(DataImportEventPayload eventPayload, MappingContext mappingContext, Reader reader, Writer writer,
                                        List<MappingRule> mappingRules, String entityType, String marcField) throws IOException {
    HashMap<String, String> payloadContext = eventPayload.getContext();
    JsonArray entities = new JsonArray();

    JsonObject originalMarcBib = new JsonObject(payloadContext.get(MARC_BIBLIOGRAPHIC));
    JsonObject content = new JsonObject(originalMarcBib.getJsonObject(PARSED_RECORD).getString(CONTENT));
    List<String> multipleEntityFields = new ArrayList<>();
    List<String> nonMultipleFields = new ArrayList<>();

    content.getJsonArray(FIELDS).forEach(e -> {
      JsonObject field = (JsonObject) e;
      if (field.getValue(marcField) != null) multipleEntityFields.add(field.toString());
      else nonMultipleFields.add(field.toString());
    });

    for (String field : multipleEntityFields) {
      JsonArray singleEntityFields = new JsonArray();
      nonMultipleFields.forEach(nonRepField -> singleEntityFields.add(new JsonObject(nonRepField)));
      singleEntityFields.add(new JsonObject(field));
      content.put(FIELDS, singleEntityFields);

      JsonObject marcBibForSingleEntity = new JsonObject(originalMarcBib.encode());
      marcBibForSingleEntity.getJsonObject(PARSED_RECORD).put(CONTENT, content.encode());
      payloadContext.put(MARC_BIBLIOGRAPHIC, marcBibForSingleEntity.encode());

      reader.initialize(eventPayload, mappingContext);
      entities.add(mapSingleEntity(eventPayload, reader, writer, mappingRules, entityType));
    }

    payloadContext.put(MARC_BIBLIOGRAPHIC, originalMarcBib.encode());
    reader.initialize(eventPayload, mappingContext);
    return entities;
  }

  default JsonObject mapSingleEntity(DataImportEventPayload eventPayload, Reader reader, Writer writer, List<MappingRule> mappingRules, String entityType) throws JsonProcessingException {
    for (MappingRule rule : mappingRules) {
      if (Boolean.parseBoolean(rule.getEnabled())) {
        Value value = reader.read(rule);
        writer.write(rule.getPath(), value);
      }
    }
    DataImportEventPayload resultedPayload = writer.getResult(eventPayload);
    return new JsonObject(resultedPayload.getContext().get(entityType));
  }
}
