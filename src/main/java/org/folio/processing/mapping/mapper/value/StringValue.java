package org.folio.processing.mapping.mapper.value;

public final class StringValue implements Value<String> {
    private String value;

    protected StringValue(String value) {
      this.value = value;
    }

    public static StringValue of(String value) {
      return new StringValue(value);
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public ValueType getType() {
        return ValueType.STRING;
    }
}
