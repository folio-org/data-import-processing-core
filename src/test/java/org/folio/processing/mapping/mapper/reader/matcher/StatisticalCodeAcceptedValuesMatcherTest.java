package org.folio.processing.mapping.mapper.reader.matcher;


import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StatisticalCodeAcceptedValuesMatcherTest {

  private StatisticalCodeAcceptedValuesMatcher acceptedValuesMatcher = new StatisticalCodeAcceptedValuesMatcher();

  @Test
  public void shouldMatchCaseSensitivelyByName() {
    String statisticalCodeAcceptedValue = "RECM (Record management): arch - Archives (arch)";
    String codeName = "Archives (arch)";
    Assert.assertTrue(acceptedValuesMatcher.matches(statisticalCodeAcceptedValue, codeName));
  }

  @Test
  public void shouldMatchCaseSensitivelyByCode() {
    String statisticalCodeAcceptedValue = "RECM (Record management): arch - Archives (arch)";
    String code = "arch";
    Assert.assertTrue(acceptedValuesMatcher.matches(statisticalCodeAcceptedValue, code));
  }

  @Test
  public void shouldNotMatchCaseInsensitivelyByName() {
    String statisticalCodeAcceptedValue = "RECM (Record management): arch - Archives (arch)";
    String codeName = "archives (arch)";
    Assert.assertFalse(acceptedValuesMatcher.matches(statisticalCodeAcceptedValue, codeName));
  }

  @Test
  public void shouldNotMatchCaseInsensitivelyByCode() {
    String statisticalCodeAcceptedValue = "RECM (Record management): arch - Archives (arch)";
    String code = "ARCH";
    Assert.assertFalse(acceptedValuesMatcher.matches(statisticalCodeAcceptedValue, code));
  }

}
