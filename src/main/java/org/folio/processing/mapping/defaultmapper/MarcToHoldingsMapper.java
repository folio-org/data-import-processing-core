package org.folio.processing.mapping.defaultmapper;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.util.List;
import java.util.stream.Collectors;

import io.vertx.core.json.JsonObject;

import org.folio.rest.jaxrs.model.ElectronicAccessItem;
import org.folio.rest.jaxrs.model.HoldingsRecord;
import org.folio.processing.mapping.defaultmapper.processor.Processor;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

public class MarcToHoldingsMapper implements RecordMapper<HoldingsRecord> {

  private static final String MARC_FORMAT = "MARC_HOLDINGS";
  private static final String MARC_SOURCE_ID = "036ee84a-6afd-4c3c-9ad3-4a12ab875f59";

  @Override
  public HoldingsRecord mapRecord(JsonObject parsedRecord, MappingParameters mappingParameters, JsonObject mappingRules) {
    HoldingsRecord holdings = new Processor<HoldingsRecord>().process(parsedRecord, mappingParameters, mappingRules, HoldingsRecord.class);
    if (holdings != null) {
      holdings = removeElectronicAccessEntriesWithNoUri(holdings);
      holdings.setSourceId(MARC_SOURCE_ID);
    }
    return holdings;
  }

  @Override
  public String getMapperFormat() {
    return MARC_FORMAT;
  }

  private HoldingsRecord removeElectronicAccessEntriesWithNoUri(HoldingsRecord holdings) {
    List<ElectronicAccessItem> electronicAccessList = holdings.getElectronicAccess().stream()
      .filter(electronicAccess -> isNotEmpty(electronicAccess.getUri()))
      .collect(Collectors.toList());
    return holdings.withElectronicAccess(electronicAccessList);
  }

}
