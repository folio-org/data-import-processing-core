package org.folio.processing.value;

import org.folio.rest.jaxrs.model.MappingRule.RepeatableFieldAction;

import java.util.List;

public class ListValue implements Value<List<String>> {
  private final List<String> list;
  private RepeatableFieldAction repeatableFieldAction;

  protected ListValue(List<String> list) {
    this.list = list;
  }

  protected ListValue(List<String> list, RepeatableFieldAction repeatableFieldAction) {
    this.list = list;
    this.repeatableFieldAction = repeatableFieldAction;
  }

  public static ListValue of(List<String> list) {
    return new ListValue(list);
  }

  public static ListValue of(List<String> list, RepeatableFieldAction repeatableFieldAction) {
    return new ListValue(list, repeatableFieldAction);
  }

  @Override
  public List<String> getValue() {
    return list;
  }

  @Override
  public ValueType getType() {
    return ValueType.LIST;
  }

  public RepeatableFieldAction getRepeatableFieldAction() {
    return repeatableFieldAction;
  }
}
