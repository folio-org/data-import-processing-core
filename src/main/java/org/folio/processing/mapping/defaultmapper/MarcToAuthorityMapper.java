package org.folio.processing.mapping.defaultmapper;

import io.vertx.core.json.JsonObject;

import org.folio.Authority;
import org.folio.processing.mapping.defaultmapper.processor.Processor;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

public class MarcToAuthorityMapper implements RecordMapper<Authority> {

  private static final String MARC_FORMAT = "MARC_AUTHORITY";

  @Override
  public Authority mapRecord(JsonObject parsedRecord, MappingParameters mappingParameters, JsonObject mappingRules) {
    return new Processor<Authority>().process(parsedRecord, mappingParameters, mappingRules, Authority.class);
  }

  @Override
  public String getMapperFormat() {
    return MARC_FORMAT;
  }

}
