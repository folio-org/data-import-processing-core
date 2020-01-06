package org.folio.processing.value;

public final class MissingValue implements Value<Object> {
  private static final MissingValue INSTANCE = new MissingValue();

  protected MissingValue() {
  }

  public static MissingValue getInstance() {
    return INSTANCE;
  }

  @Override
  public Object getValue() {
    return null;
  }

  @Override
  public ValueType getType() {
    return ValueType.MISSING;
  }
}
