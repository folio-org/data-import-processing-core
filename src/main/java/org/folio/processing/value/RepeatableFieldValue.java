package org.folio.processing.value;

import org.folio.rest.jaxrs.model.MappingRule;

import java.util.List;
import java.util.Map;

import static org.folio.processing.value.Value.ValueType.REPEATABLE;

public class RepeatableFieldValue implements Value<List<Map<String, Value>>> {

  private List<Map<String, Value>> value;
  private MappingRule.RepeatableFieldAction repeatableFieldAction;
  private String rootPath;

  protected RepeatableFieldValue(List<Map<String, Value>> value, MappingRule.RepeatableFieldAction action, String rootPath) {
    this.value = value;
    this.repeatableFieldAction = action;
    this.rootPath = rootPath;
  }

  public static RepeatableFieldValue of(List<Map<String, Value>> value, MappingRule.RepeatableFieldAction action, String rootPath) {
    return new RepeatableFieldValue(value, action, rootPath);
  }

  @Override
  public List<Map<String, Value>> getValue() {
    return value;
  }

  public MappingRule.RepeatableFieldAction getRepeatableFieldAction() {
    return repeatableFieldAction;
  }

  public String getRootPath() {
    return rootPath;
  }

  @Override
  public ValueType getType() {
    return REPEATABLE;
  }
}
