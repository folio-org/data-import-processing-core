package org.folio.processing.mapping.mapper.mappers;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import static org.folio.processing.mapping.mapper.reader.record.marc.MarcRecordReader.STRING_VALUE_PATTERN;

public class HoldingsMapper implements Mapper {
  private static final Logger LOGGER = LogManager.getLogger(HoldingsMapper.class);
  private static final String PERMANENT_LOCATION_ID = "permanentLocationId";
  private static final String HOLDINGS = "HOLDINGS";
  public static final String HOLDINGS_IDENTIFIERS = "HOLDINGS_IDENTIFIERS";
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
      return executeMultipleHoldingsLogic(eventPayload, profile, mappingContext);
    } catch (IOException e) {
      LOGGER.warn("map:: Failed to perform Holdings mapping", e);
      throw new MappingException(e);
    }
  }

  private DataImportEventPayload executeMultipleHoldingsLogic(DataImportEventPayload eventPayload, MappingProfile profile, MappingContext mappingContext) throws IOException {
    List<MappingRule> mappingRules = profile.getMappingDetails().getMappingFields();
    JsonArray holdings = new JsonArray();
    Optional<MappingRule> permanentLocationMappingRule = mappingRules.stream().filter(rule -> rule.getName().equals(PERMANENT_LOCATION_ID)).findFirst();

    if (permanentLocationMappingRule.isEmpty() || STRING_VALUE_PATTERN.matcher(permanentLocationMappingRule.get().getValue()).matches()) {
      holdings.add(mapSingleEntity(eventPayload, reader, writer, mappingRules, HOLDINGS));
    } else {
      String marcField = retrieveMarcFieldFromMappingRule(permanentLocationMappingRule.get())
        .orElseThrow(() -> new RuntimeException(String.format("Invalid  value for mapping rule: %s", PERMANENT_LOCATION_ID)));
      eventPayload.getContext().put("REPEATABLE_HOLDINGS_FIELD", marcField);

      holdings = mapMultipleEntities(eventPayload, mappingContext, reader, writer, mappingRules, HOLDINGS, marcField);
    }
    eventPayload.getContext().put(HOLDINGS_IDENTIFIERS, Json.encode(getPermanentLocationsFromHoldings(holdings)));
    eventPayload.getContext().put(HOLDINGS, Json.encode(distinctHoldingsByPermanentLocation(holdings)));
    return eventPayload;
  }

  private List<JsonObject> distinctHoldingsByPermanentLocation(JsonArray holdings) {
    List<JsonObject> distinctHoldings = new ArrayList<>();
    for (int i = 0; i < holdings.size(); i++) {
      JsonObject holdingsAsJson = getHoldingsAsJson(holdings.getJsonObject(i));
      String holdingPermanentLocation = holdingsAsJson.getString(PERMANENT_LOCATION_ID);

      if (distinctHoldings.stream().noneMatch(hol -> StringUtils.equals(getHoldingsAsJson(hol).getString(PERMANENT_LOCATION_ID), holdingPermanentLocation))) {
        distinctHoldings.add(holdings.getJsonObject(i));
      }
    }
    return distinctHoldings;
  }

  private JsonObject getHoldingsAsJson(JsonObject holdings) {
    if (holdings.getJsonObject("holdings") != null) {
      return holdings.getJsonObject("holdings");
    }
    return holdings;
  }

  private List<String> getPermanentLocationsFromHoldings(JsonArray holdings) {
    List<String> permanentLocationsIds = new ArrayList<>();
    holdings.forEach(e -> {
      JsonObject holdingsAsJson = getHoldingsAsJson((JsonObject) e);
      permanentLocationsIds.add(holdingsAsJson.getString(PERMANENT_LOCATION_ID));
    });
    return permanentLocationsIds;
  }

  private Optional<String> retrieveMarcFieldFromMappingRule(MappingRule mappingRule) {
    String[] marcFields = mappingRule.getValue().split("\\$");
    if (marcFields.length > 0) {
      return Optional.of(marcFields[0]);
    }
    return Optional.empty();
  }
}
