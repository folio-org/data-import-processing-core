package org.folio.processing.mapping.defaultmapper.processor.functions.enums;

import org.apache.commons.lang3.StringUtils;

public enum CallNumberTypesEnum {
  LIBRARY_OF_CONGRESS_CLASSIFICATION('0', "library of congress classification"),
  DEWEY_DECIMAL_CLASSIFICATION('1', "dewey decimal classification"),
  NATIONAL_LIBRARY_OF_MEDICINE_CLASSIFICATION('2', "national library of medicine classification"),
  SUPERINTENDENT_OF_DOCUMENTS_CLASSIFICATION('3', "superintendent of documents classification"),
  SHELVING_CONTROL_NUMBER('4', "shelving control number"),
  TITLE('5', "title"),
  SHELVED_SEPARATELY('6', "shelved separately"),
  SOURCE_SPECIFIED_IN_SUBFIELD_2('7', "source specified in subfield $2"),
  OTHER_SCHEME('8', "other scheme");

  CallNumberTypesEnum(char indicator1, String name) {
    this.indicator1 = indicator1;
    this.name = name;
  }

  private char indicator1;
  private String name;

  public static String getNameByIndicator(char indicatorValue) {
    for (CallNumberTypesEnum enumValue : values()) {
      if (indicatorValue == enumValue.indicator1) {
        return enumValue.name;
      }
    }
    return StringUtils.EMPTY;
  }
}
