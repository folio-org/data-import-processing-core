package org.folio.processing.mapping.mapper.reader.utils;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum RequiredFields {

  ELECTRONIC_ACCESS_URI("uri");

  private String fieldName;

  private static Map<String, RequiredFields> nameToRequiredField = getNameToRequiredFieldMap();

  RequiredFields(String fieldName) {
    this.fieldName = fieldName;
  }

  public static boolean isRequiredFieldName(String fieldName) {
    return nameToRequiredField.containsKey(fieldName);
  }

  private static Map<String, RequiredFields> getNameToRequiredFieldMap() {
    return Arrays.stream(RequiredFields.values())
      .collect(Collectors.toMap(field -> field.fieldName, field -> field));
  }
}
