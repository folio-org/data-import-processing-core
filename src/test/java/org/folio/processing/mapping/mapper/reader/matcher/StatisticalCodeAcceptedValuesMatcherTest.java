package org.folio.processing.mapping.mapper.reader.matcher;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StatisticalCodeAcceptedValuesMatcherTest {

  private StatisticalCodeAcceptedValuesMatcher acceptedValuesMatcher = new StatisticalCodeAcceptedValuesMatcher();

  @Test
  void shouldMatchCaseSensitivelyByName() {
    String statisticalCodeAcceptedValue = "RECM (Record management): arch - Archives (arch)";
    String codeName = "Archives (arch)";
    Assertions.assertTrue(acceptedValuesMatcher.matches(statisticalCodeAcceptedValue, codeName));
  }

  @Test
  void shouldMatchCaseSensitivelyByCode() {
    String statisticalCodeAcceptedValue = "RECM (Record management): arch - Archives (arch)";
    String code = "arch";
    Assertions.assertTrue(acceptedValuesMatcher.matches(statisticalCodeAcceptedValue, code));
  }

  @Test
  void shouldNotMatchCaseInsensitivelyByName() {
    String statisticalCodeAcceptedValue = "RECM (Record management): arch - Archives (arch)";
    String codeName = "archives (arch)";
    Assertions.assertFalse(acceptedValuesMatcher.matches(statisticalCodeAcceptedValue, codeName));
  }

  @Test
  void shouldNotMatchCaseInsensitivelyByCode() {
    String statisticalCodeAcceptedValue = "RECM (Record management): arch - Archives (arch)";
    String code = "ARCH";
    Assertions.assertFalse(acceptedValuesMatcher.matches(statisticalCodeAcceptedValue, code));
  }

}
