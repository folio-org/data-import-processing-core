package org.folio.processing.mapping.defaultmapper;

import io.vertx.core.json.JsonObject;
import org.folio.Instance;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

/**
 * Common interface for Record to Instance mapper. Mappers for each format of Parsed Record should implement it
 */
public interface RecordToInstanceMapper {

  /**
   * Maps Parsed Record to Instance Record
   *
   * @param parsedRecord      - JsonObject containing Parsed Record
   * @param mappingParameters - parameters needed for mapping functions
   * @param mappingRules      - required rules for mapping
   * @return - Wrapper for parsed record in json format.
   * Can contains errors descriptions if parsing was failed
   */
  Instance mapRecord(JsonObject parsedRecord, MappingParameters mappingParameters, JsonObject mappingRules);

  /**
   * Provides access to the MapperFormat
   *
   * @return - format which RecordMapper can map
   */
  String getMapperFormat();
}
