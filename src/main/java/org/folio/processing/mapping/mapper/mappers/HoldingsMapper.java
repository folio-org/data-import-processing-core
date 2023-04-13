package org.folio.processing.mapping.mapper.mappers;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
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
import org.folio.processing.value.ListValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.MappingRule;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class HoldingsMapper implements Mapper {

  private static final Logger LOGGER = LogManager.getLogger(MappingManager.class);
  public static final String PERMANENT_LOCATION_ID = "permanentLocationId";
  public static final String HOLDINGS = "HOLDINGS";

  private Reader reader;
  private Writer writer;

  public HoldingsMapper(Reader reader, Writer writer) {
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
      return executeMultipleHoldingsLogic(eventPayload, profile);
    } catch (IOException e) {
      LOGGER.warn("map:: Failed to perform Holdings mapping", e);
      throw new MappingException(e);
    }
  }

  private DataImportEventPayload executeMultipleHoldingsLogic(DataImportEventPayload eventPayload, MappingProfile profile) throws IOException {
    List<MappingRule> mappingRules = profile.getMappingDetails().getMappingFields();

    ListValue permanentLocationIds = null;
    for (MappingRule rule : mappingRules) {
      if (Boolean.parseBoolean(rule.getEnabled())) {
        if (StringUtils.equals(rule.getName(), PERMANENT_LOCATION_ID)) {
          permanentLocationIds = (ListValue) reader.read(rule);
        }
      }
    }
    List<MappingRule> mappingRulesWithoutPermanentLocation = filterRulesByPermanentLocationId(mappingRules);
    for (MappingRule rule : mappingRulesWithoutPermanentLocation) {
      if (Boolean.parseBoolean(rule.getEnabled())) {
        Value value = reader.read(rule);
        writer.write(rule.getPath(), value);
      }
    }
    DataImportEventPayload result = writer.getResult(eventPayload);
    JsonObject originalHolding = new JsonObject(result.getContext().get(HOLDINGS));
    JsonArray holdings = new JsonArray();
    if (permanentLocationIds != null) {
      for (String location : permanentLocationIds.getValue()) {
        holdings.add(originalHolding.copy().put(PERMANENT_LOCATION_ID, location));
      }
    }
    eventPayload.getContext().put(HOLDINGS, holdings.encode());
    return eventPayload;
  }

  private static List<MappingRule> filterRulesByPermanentLocationId(List<MappingRule> rules) {
    return rules.stream()
      .filter(mappingRule -> !PERMANENT_LOCATION_ID.equals(mappingRule.getName()))
      .collect(Collectors.toList());
  }
}
