package org.folio.processing.mapping.defaultmapper;

import io.vertx.core.json.JsonObject;
import org.folio.Authority;
import org.folio.AuthorityExtended;
import org.folio.processing.mapping.defaultmapper.processor.Processor;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

public class MarkToAuthorityExtendedMapper extends MarcToAuthorityMapper {

  private static final String MARC_FORMAT = "MARC_AUTHORITY_EXTENDED";

  @Override
  public String getMapperFormat() {
    return MARC_FORMAT;
  }

  @Override
  public Authority mapRecord(JsonObject parsedRecord, MappingParameters mappingParameters, JsonObject mappingRules) {
    var authority = new Processor<AuthorityExtended>().process(parsedRecord, mappingParameters, mappingRules,
      AuthorityExtended.class);
    linkSourceFile(parsedRecord, mappingParameters, authority);
    return authority;
  }
}
