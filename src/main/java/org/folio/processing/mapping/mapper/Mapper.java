package org.folio.processing.mapping.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
  String EMPTY_JSON = "{}";

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

  default JsonArray mapMultipleEntitiesByMarcField(DataImportEventPayload eventPayload, MappingContext mappingContext, Reader reader, Writer writer,
                                                   List<MappingRule> mappingRules, String entityType, String marcField) throws IOException {
    HashMap<String, String> payloadContext = eventPayload.getContext();
    JsonArray entities = new JsonArray();

    JsonObject originalMarcBib = new JsonObject(payloadContext.get(MARC_BIBLIOGRAPHIC));
    JsonObject content = new JsonObject(originalMarcBib.getJsonObject(PARSED_RECORD).getString(CONTENT));
    List<JsonObject> multipleEntityFields = new ArrayList<>();
    List<JsonObject> nonMultipleFields = new ArrayList<>();

    content.getJsonArray(FIELDS).forEach(e -> {
      JsonObject field = (JsonObject) e;
      if (field.getValue(marcField) != null) multipleEntityFields.add(new JsonObject(field.toString()));
      else nonMultipleFields.add(new JsonObject(field.toString()));
    });

    for (JsonObject field : multipleEntityFields) {
      List<JsonObject> singleEntityFields = new ArrayList<>(nonMultipleFields);
      singleEntityFields.add(field);
      content.put(FIELDS, singleEntityFields);

      JsonObject marcBibForSingleEntity = originalMarcBib.copy();
      marcBibForSingleEntity.getJsonObject(PARSED_RECORD).put(CONTENT, content.encode());
      payloadContext.put(entityType, EMPTY_JSON);
      payloadContext.put(MARC_BIBLIOGRAPHIC, marcBibForSingleEntity.encode());

      reader.initialize(eventPayload, mappingContext);
      writer.initialize(eventPayload);
      entities.add(mapSingleEntity(eventPayload, reader, writer, mappingRules, entityType));
    }

    payloadContext.put(MARC_BIBLIOGRAPHIC, originalMarcBib.encode());
    reader.initialize(eventPayload, mappingContext);
    writer.initialize(eventPayload);
    return entities;
  }

  default JsonObject mapSingleEntity(DataImportEventPayload eventPayload, Reader reader, Writer writer,
                                     List<MappingRule> mappingRules, String entityType) throws JsonProcessingException {
    for (MappingRule rule : mappingRules) {
      if (Boolean.parseBoolean(rule.getEnabled())) {
        Value value = reader.read(rule);
        writer.write(rule.getPath(), value);
      }
    }
    DataImportEventPayload resultedPayload = writer.getResult(eventPayload);
    return new JsonObject(resultedPayload.getContext().get(entityType));
  }

  default void adjustContextToContainEntitiesAsJsonObject(DataImportEventPayload eventPayload, EntityType entityType) {
    if (isJsonArray(eventPayload.getContext().get(entityType.value()))) {
      JsonArray entities = new JsonArray(eventPayload.getContext().get(entityType.value()));
      if (entities.size() > 0) {
        eventPayload.getContext().put(entityType.value(), entities.getJsonObject(0).encode());
      } else {
        eventPayload.getContext().put(entityType.value(), EMPTY_JSON);
      }
    }
  }

  default boolean isJsonArray(String jsonArrayAsString) {
    try {
      new JsonArray(jsonArrayAsString);
      return true;
    } catch (DecodeException e) {
      return false;
    }
  }
}
