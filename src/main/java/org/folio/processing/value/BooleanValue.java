package org.folio.processing.value;

import org.folio.rest.jaxrs.model.MappingRule;

public class BooleanValue implements Value<MappingRule.BooleanFieldAction> {

  private MappingRule.BooleanFieldAction booleanFieldAction;

  protected BooleanValue(MappingRule.BooleanFieldAction value) {
    this.booleanFieldAction = value;
  }

  public static BooleanValue of(MappingRule.BooleanFieldAction value) {
    return new BooleanValue(value);
  }

  @Override
  public MappingRule.BooleanFieldAction getValue() {
    return booleanFieldAction;
  }

  @Override
  public ValueType getType() {
    return ValueType.BOOLEAN;
  }
}
