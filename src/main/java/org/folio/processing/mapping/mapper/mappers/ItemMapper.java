package org.folio.processing.mapping.mapper.mappers;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.exceptions.MappingException;
import org.folio.processing.mapping.mapper.Mapper;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.rest.jaxrs.model.MappingRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static org.folio.processing.mapping.mapper.mappers.HoldingsMapper.MULTIPLE_HOLDINGS_FIELD;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;

public class ItemMapper implements Mapper {
  private static final Logger LOGGER = LogManager.getLogger(ItemMapper.class);
  private final Reader reader;
  private final Writer writer;

  public ItemMapper(Reader reader, Writer writer) {
    this.reader = reader;
    this.writer = writer;
  }

  @Override
  public DataImportEventPayload map(MappingProfile profile, DataImportEventPayload eventPayload, MappingContext mappingContext) {
    try {
      initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
      if (ifProfileIsInvalid(profile)) {
        return eventPayload;
      }
      return executeMultipleItemsLogic(eventPayload, profile, mappingContext);
    } catch (IOException e) {
      LOGGER.warn("map:: Failed to perform Items mapping", e);
      throw new MappingException(e);
    }
  }

  private DataImportEventPayload executeMultipleItemsLogic(DataImportEventPayload eventPayload, MappingProfile profile,
                                                           MappingContext mappingContext) throws IOException {
    HashMap<String, String> payloadContext = eventPayload.getContext();
    List<MappingRule> mappingRules = profile.getMappingDetails().getMappingFields();
    JsonArray items = new JsonArray();
    if (isJsonArray(payloadContext.get(ITEM.value())) && !new JsonArray(payloadContext.get(ITEM.value())).isEmpty()) {
      mapMultipleItemIfItemEntityExistsInContext(eventPayload, mappingContext, payloadContext, mappingRules, items);
    } else {
      String marcField = payloadContext.get(MULTIPLE_HOLDINGS_FIELD);
      if (marcField == null) {
        adjustContextToContainEntitiesAsJsonObject(eventPayload, ITEM);
        writer.initialize(eventPayload);
        items.add(mapSingleEntity(eventPayload, reader, writer, mappingRules, ITEM.value()));
      } else {
        items = mapMultipleEntitiesByMarcField(eventPayload, mappingContext, reader, writer, mappingRules, ITEM.value(), marcField);
        payloadContext.remove(MULTIPLE_HOLDINGS_FIELD);
      }
    }
    payloadContext.put(ITEM.value(), Json.encode(items));
    return eventPayload;
  }

  private void mapMultipleItemIfItemEntityExistsInContext(DataImportEventPayload eventPayload, MappingContext mappingContext, HashMap<String, String> payloadContext, List<MappingRule> mappingRules, JsonArray items) throws IOException {
    JsonArray itemList = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    for (int i = 0; i < itemList.size(); i++) {
      JsonObject currentItem = itemList.getJsonObject(i);
      eventPayload.getContext().put(ITEM.value(), currentItem.encode());
      reader.initialize(eventPayload, mappingContext);
      writer.initialize(eventPayload);
      items.add(mapSingleEntity(eventPayload, reader, writer, mappingRules, ITEM.value()));
    }
    payloadContext.remove(MULTIPLE_HOLDINGS_FIELD);
  }
}
