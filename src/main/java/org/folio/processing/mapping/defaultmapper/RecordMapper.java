package org.folio.processing.mapping.defaultmapper;

import io.vertx.core.json.JsonObject;
import org.apache.commons.collections4.CollectionUtils;
import org.folio.Instance;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

import java.util.Collections;
import java.util.List;

/**
 * Common interface for Record to Instance mapper. Mappers for each format of Parsed Record should implement it
 */
public interface RecordMapper<T> {

  default List<T> mapRecords(List<JsonObject> parsedRecords,
                             MappingParameters mappingParameters,
                             JsonObject mappingRules) {
    if (CollectionUtils.isEmpty(parsedRecords)) {
      return Collections.emptyList();
    }

    return parsedRecords.stream()
      .map(parsedRecord -> mapRecord(parsedRecord, mappingParameters, mappingRules))
      .toList();
  }

  /**
   * Maps Parsed Record to Instance Record
   *
   * @param parsedRecord      - JsonObject containing Parsed Record
   * @param mappingParameters - parameters needed for mapping functions
   * @param mappingRules      - required rules for mapping
   * @return - Wrapper for parsed record in json format.
   * Can contains errors descriptions if parsing was failed
   */
  T mapRecord(JsonObject parsedRecord, MappingParameters mappingParameters, JsonObject mappingRules);

  /**
   * Provides access to the MapperFormat
   *
   * @return - format which RecordMapper can map
   */
  String getMapperFormat();
}
