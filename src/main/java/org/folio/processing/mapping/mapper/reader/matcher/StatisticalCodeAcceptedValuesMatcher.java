package org.folio.processing.mapping.mapper.reader.matcher;

import org.apache.commons.lang3.StringUtils;

public class StatisticalCodeAcceptedValuesMatcher implements AcceptedValuesMatcher {

  @Override
  public boolean matches(String acceptedValue, String valueToCompare) {
    return matchesByCode(acceptedValue, valueToCompare)
      || matchesByName(acceptedValue, valueToCompare);
  }

  private boolean matchesByCode(String acceptedValue, String valueToCompare) {
    String code = StringUtils.substringBetween(acceptedValue, ": ", " - ");
    return valueToCompare.equalsIgnoreCase(code);
  }

  private boolean matchesByName(String acceptedValue, String valueToCompare) {
    String statisticalCodeName = StringUtils.substringAfter(acceptedValue, " - ");
    return valueToCompare.equalsIgnoreCase(statisticalCodeName);
  }

}
