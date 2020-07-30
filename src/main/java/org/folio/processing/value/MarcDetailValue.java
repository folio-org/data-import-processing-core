package org.folio.processing.value;

import org.folio.rest.jaxrs.model.MarcMappingDetail;

public class MarcDetailValue implements Value<MarcMappingDetail> {

  private MarcMappingDetail marcMappingDetail;

  protected MarcDetailValue(MarcMappingDetail marcMappingDetail) {
    this.marcMappingDetail = marcMappingDetail;
  }

  public static MarcDetailValue of(MarcMappingDetail marcMappingDetail) {
    return new MarcDetailValue(marcMappingDetail);
  }

  @Override
  public MarcMappingDetail getValue() {
    return marcMappingDetail;
  }

  @Override
  public ValueType getType() {
    return ValueType.MARC_DETAIL;
  }
}
