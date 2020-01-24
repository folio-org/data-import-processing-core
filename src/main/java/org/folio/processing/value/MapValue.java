package org.folio.processing.value;

import java.util.HashMap;
import java.util.Map;

public class MapValue implements Value<Map<String, String>> {
  private Map<String, String> value;

  public MapValue() {
    this.value = new HashMap<>();
  }

  public MapValue putEntry(String entryKey, String entryValue) {
    value.put(entryKey, entryValue);
    return this;
  }

  @Override
  public Map<String, String> getValue() {
    return value;
  }

  @Override
  public ValueType getType() {
    return ValueType.MAP;
  }
}
