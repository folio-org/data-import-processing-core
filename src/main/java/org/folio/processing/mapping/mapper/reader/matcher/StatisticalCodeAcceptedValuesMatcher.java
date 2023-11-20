package org.folio.processing.mapping.mapper.reader.matcher;

import org.apache.commons.lang3.StringUtils;

public class StatisticalCodeAcceptedValuesMatcher implements AcceptedValuesMatcher {

  private static final String CODE_SEPARATOR = ": ";
  private static final String NAME_SEPARATOR = " - ";

  @Override
  public boolean matches(String acceptedValue, String valueToCompare) {
    return matchesByCode(acceptedValue, valueToCompare)
      || matchesByName(acceptedValue, valueToCompare);
  }

  private boolean matchesByCode(String acceptedValue, String valueToCompare) {
    String code = StringUtils.substringBetween(acceptedValue, CODE_SEPARATOR, NAME_SEPARATOR);
    return valueToCompare.equals(code);
  }

  private boolean matchesByName(String acceptedValue, String valueToCompare) {
    String statisticalCodeName = StringUtils.substringAfter(acceptedValue, NAME_SEPARATOR);
    return valueToCompare.equals(statisticalCodeName);
  }

}
