package org.folio.processing.mapping.defaultmapper.processor.functions.enums;

public enum HoldingsTypeEnum {

  MULTI_PART_MONOGRAPH('v', "multi-part monograph"),
  MONOGRAPH('x', "monograph"),
  SERIAL('y', "serial"),
  UNKNOWN('u', "unknown");

  private char symbol;
  private String name;

  HoldingsTypeEnum(char symbol, String name) {
    this.symbol = symbol;
    this.name = name;
  }

  public String getValue(){
    return name;
  }

  public static String getNameByCharacter(char marcValue) {
    for (HoldingsTypeEnum enumValue : values()) {
      if (marcValue == enumValue.symbol) {
        return enumValue.name;
      }
    }
    return UNKNOWN.name;
  }
}
