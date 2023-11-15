package org.folio.processing.mapping.mapper.reader.matcher;


import org.junit.Assert;
import org.junit.Test;

public class StatisticalCodeAcceptedValuesMatcherTest {

  private StatisticalCodeAcceptedValuesMatcher acceptedValuesMatcher = new StatisticalCodeAcceptedValuesMatcher();

  @Test
  public void shouldMatchByName() {
    String statisticalCodeAcceptedValue = "RECM (Record management): arch - Archives (arch)";
    String codeName = "Archives (arch)";
    Assert.assertTrue(acceptedValuesMatcher.matches(statisticalCodeAcceptedValue, codeName));
  }

  @Test
  public void shouldMatchByCode() {
    String statisticalCodeAcceptedValue = "RECM (Record management): arch - Archives (arch)";
    String code = "arch";
    Assert.assertTrue(acceptedValuesMatcher.matches(statisticalCodeAcceptedValue, code));
  }

  @Test
  public void shouldMatchByNameExcludingCode() {
    String statisticalCodeAcceptedValue = "RECM (Record management): arch - Archives (arch)";
    String nameWithoutCodePart = "Archives";
    Assert.assertTrue(acceptedValuesMatcher.matches(statisticalCodeAcceptedValue, nameWithoutCodePart));
  }

}
