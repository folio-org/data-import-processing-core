package org.folio.processing.mapping.mapper;

import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

public class MappingContext {

  MappingParameters mappingParameters = new MappingParameters();

  public MappingParameters getMappingParameters() {
    return mappingParameters;
  }

  public void setMappingParameters(MappingParameters mappingParameters) {
    this.mappingParameters = mappingParameters;
  }

  public MappingContext withMappingParameters(MappingParameters mappingParameters) {
    this.mappingParameters = mappingParameters;
    return this;
  }
}
