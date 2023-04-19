package org.folio.processing.mapping.mapper.mappers;

import com.fasterxml.jackson.core.JsonProcessingException;
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
  private static final Logger LOGGER = LogManager.getLogger(HoldingsMapper.class);
  public static final String PERMANENT_LOCATION_ID = "permanentLocationId";

  public static final String HOLDINGS = "HOLDINGS";
  public static final String IF_DUPLICATES_NEEDED = "ifDuplicatesNeeded";
  public static final String HOLDINGS_IDENTIFIERS = "HOLDINGS_IDENTIFIERS";
  public static final String HOLDINGS_PROPERTY = "holdings";
  private Reader reader;
  private Writer writer;

  public HoldingsMapper(Reader reader, Writer writer) {
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
      return executeMultipleHoldingsLogic(eventPayload, profile);
    } catch (IOException e) {
      LOGGER.warn("map:: Failed to perform Holdings mapping", e);
      throw new MappingException(e);
    }
  }

  private DataImportEventPayload executeMultipleHoldingsLogic(DataImportEventPayload eventPayload, MappingProfile profile) throws IOException {
    List<MappingRule> mappingRules = profile.getMappingDetails().getMappingFields();
    ListValue permanentLocationIdsWithDuplicates = constructLocationsWithDuplicates(eventPayload, mappingRules);

    if (permanentLocationIdsWithDuplicates != null && permanentLocationIdsWithDuplicates.getValue() != null) {
      List<String> locationsWithDuplicates = permanentLocationIdsWithDuplicates.getValue();

      JsonArray holdingsIdentifier = new JsonArray(locationsWithDuplicates);
      eventPayload.getContext().put(HOLDINGS_IDENTIFIERS, holdingsIdentifier.toString());
      List<String> uniquePermanentLocationIds = locationsWithDuplicates
        .stream()
        .distinct()
        .collect(Collectors.toList());

      JsonObject originalHolding = mapDefaultHolding(eventPayload, mappingRules);
      JsonArray holdings = new JsonArray();
      for (String location : uniquePermanentLocationIds) {
        JsonObject coreHolding = originalHolding.getJsonObject(HOLDINGS_PROPERTY);
        JsonObject copiedHoldings = originalHolding.copy();
        if (coreHolding == null) { //In case if there are no other rules, just for permanentLocationIds.
          copiedHoldings.put(HOLDINGS_PROPERTY, new JsonObject().put(PERMANENT_LOCATION_ID, location));
          holdings.add(copiedHoldings);
        } else {
          copiedHoldings.getJsonObject(HOLDINGS_PROPERTY).put(PERMANENT_LOCATION_ID, location);
          holdings.add(copiedHoldings);
        }
      }
      eventPayload.getContext().put(HOLDINGS, holdings.encode());
    }
    return eventPayload;
  }

  private JsonObject mapDefaultHolding(DataImportEventPayload eventPayload, List<MappingRule> mappingRules) throws JsonProcessingException {
    List<MappingRule> mappingRulesWithoutPermanentLocation = filterRulesByPermanentLocationId(mappingRules);
    for (MappingRule rule : mappingRulesWithoutPermanentLocation) {
      if (Boolean.parseBoolean(rule.getEnabled())) {
        Value value = reader.read(rule);
        writer.write(rule.getPath(), value);
      }
    }
    DataImportEventPayload resultedPayload = writer.getResult(eventPayload);
    return new JsonObject(resultedPayload.getContext().get(HOLDINGS));
  }

  private ListValue constructLocationsWithDuplicates(DataImportEventPayload eventPayload, List<MappingRule> mappingRules) {
    ListValue permanentLocationIdsWithDuplicates = null;
    eventPayload.getContext().put(IF_DUPLICATES_NEEDED, "true"); //For execute reading but with duplicates values.
    for (MappingRule rule : mappingRules) {
      if (Boolean.parseBoolean(rule.getEnabled()) && StringUtils.equals(rule.getName(), PERMANENT_LOCATION_ID)) {
        permanentLocationIdsWithDuplicates = (ListValue) reader.read(rule);
      }
    }
    eventPayload.getContext().remove(IF_DUPLICATES_NEEDED);
    return permanentLocationIdsWithDuplicates;
  }

  private static List<MappingRule> filterRulesByPermanentLocationId(List<MappingRule> rules) {
    return rules.stream()
      .filter(mappingRule -> !PERMANENT_LOCATION_ID.equals(mappingRule.getName()))
      .collect(Collectors.toList());
  }
}
