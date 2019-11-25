package org.folio.processing.mapping.mapper.value;

import java.util.List;

public class ListValue implements Value<List<String>> {
    private final List<String> list;

    public ListValue(List<String> list) {
        this.list = list;
    }

    @Override
    public List<String> getValue() {
        return list;
    }

    @Override
    public ValueType getType() {
        return ValueType.LIST;
    }
}
