package org.folio.processing.value;

public final class StringValue implements Value<String> {
  private final String value;
  private boolean removeOnWrite;

  protected StringValue(String value, boolean removeOnWrite) {
    this.value = value;
    this.removeOnWrite = removeOnWrite;
  }

  public static StringValue of(String value) {
    return new StringValue(value, false);
  }

  public static StringValue of(String value, boolean removeOnWrite) {
    return new StringValue(value, removeOnWrite);
  }

  @Override
  public String getValue() {
    return value;
  }

  @Override
  public ValueType getType() {
    return ValueType.STRING;
  }

  public boolean shouldRemoveOnWrite() {
    return removeOnWrite;
  }
}
