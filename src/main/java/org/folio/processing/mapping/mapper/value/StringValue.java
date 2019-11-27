package org.folio.processing.mapping.mapper.value;

public class StringValue implements Value<String> {
    private String value;

    public StringValue(String value) {
        this.value = value;
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
