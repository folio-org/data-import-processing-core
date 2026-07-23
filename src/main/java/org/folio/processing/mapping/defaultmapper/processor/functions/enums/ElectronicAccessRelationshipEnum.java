package org.folio.processing.mapping.defaultmapper.processor.functions.enums;

public enum ElectronicAccessRelationshipEnum {
  RESOURCE('0', "resource"),
  VERSION_OF_RESOURCE('1', "version of resource"),
  RELATED_RESOURCE('2', "related resource"),
  NO_INFORMATION_PROVIDED('3', "no information provided");

  private final char indicator2value;
  private final String name;
  ElectronicAccessRelationshipEnum(char indicator2value, String name) {
    this.indicator2value = indicator2value;
    this.name = name;
  }

  public static String getNameByIndicator(char indicatorValue) {
    for (ElectronicAccessRelationshipEnum enumValue : values()) {
      if (indicatorValue == enumValue.indicator2value) {
        return enumValue.name;
      }
    }
    return NO_INFORMATION_PROVIDED.name;
  }
}
