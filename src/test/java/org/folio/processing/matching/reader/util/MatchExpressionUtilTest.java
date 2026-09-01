package org.folio.processing.matching.reader.util;

import static org.folio.processing.matching.reader.util.MatchExpressionUtil.extractComparisonPart;
import static org.folio.processing.matching.reader.util.MatchExpressionUtil.isQualified;
import static org.folio.rest.jaxrs.model.Qualifier.ComparisonPart.ALPHANUMERICS_ONLY;
import static org.folio.rest.jaxrs.model.Qualifier.ComparisonPart.NUMERICS_ONLY;
import static org.folio.rest.jaxrs.model.Qualifier.QualifierType.BEGINS_WITH;
import static org.folio.rest.jaxrs.model.Qualifier.QualifierType.CONTAINS;
import static org.folio.rest.jaxrs.model.Qualifier.QualifierType.ENDS_WITH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.folio.rest.jaxrs.model.Qualifier;
import org.folio.rest.jaxrs.model.Qualifier.QualifierType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class MatchExpressionUtilTest {

  private static final String LCCN = "n  78004349 ";

  @ParameterizedTest(name = "{0} without a qualifier value")
  @EnumSource(QualifierType.class)
  void shouldQualifyValue_WhenQualifierValueIsNull(QualifierType qualifierType) {
    Qualifier qualifier = new Qualifier().withQualifierType(qualifierType);

    assertTrue(isQualified(LCCN, qualifier));
  }

  @ParameterizedTest(name = "{0} with a blank qualifier value")
  @EnumSource(QualifierType.class)
  void shouldQualifyValue_WhenQualifierValueIsEmpty(QualifierType qualifierType) {
    Qualifier qualifier = new Qualifier()
      .withQualifierType(qualifierType)
      .withQualifierValue("");

    assertTrue(isQualified(LCCN, qualifier));
  }

  @Test
  void shouldQualifyValue_WhenQualifierHasComparisonPartOnly() {
    Qualifier qualifier = new Qualifier().withComparisonPart(NUMERICS_ONLY);

    assertTrue(isQualified(LCCN, qualifier));
  }

  @Test
  void shouldQualifyValue_WhenQualifierIsNull() {
    assertTrue(isQualified(LCCN, null));
  }

  @Test
  void shouldNotThrow_WhenValueIsNull() {
    // MarcValueReaderUtil only ever passes textual subfield values, so null is not reachable in practice.
    Qualifier qualifier = new Qualifier()
      .withQualifierType(CONTAINS)
      .withQualifierValue("78004349");

    assertTrue(isQualified(null, qualifier));
  }

  @Test
  void shouldApplyBeginsWithQualifier() {
    Qualifier qualifier = new Qualifier()
      .withQualifierType(BEGINS_WITH)
      .withQualifierValue("n");

    assertTrue(isQualified("n  78004349 ", qualifier));
    assertFalse(isQualified("sh  78004349 ", qualifier));
  }

  @Test
  void shouldApplyEndsWithQualifier() {
    Qualifier qualifier = new Qualifier()
      .withQualifierType(ENDS_WITH)
      .withQualifierValue("349");

    assertTrue(isQualified("n  78004349", qualifier));
    assertFalse(isQualified("n  78004349 ", qualifier));
  }

  @Test
  void shouldApplyContainsQualifier() {
    Qualifier qualifier = new Qualifier()
      .withQualifierType(CONTAINS)
      .withQualifierValue("78004349");

    assertTrue(isQualified(LCCN, qualifier));
    assertFalse(isQualified("n  99006539 ", qualifier));
  }

  @Test
  void shouldExtractNumericsOnly_WhenQualifierHasNoQualifierType() {
    Qualifier qualifier = new Qualifier().withComparisonPart(NUMERICS_ONLY);

    assertEquals("78004349", extractComparisonPart(LCCN, qualifier));
    assertEquals("78004349", extractComparisonPart("n 78004349", qualifier));
  }

  @Test
  void shouldExtractAlphanumericsOnly_WhenQualifierHasNoQualifierType() {
    Qualifier qualifier = new Qualifier().withComparisonPart(ALPHANUMERICS_ONLY);

    assertEquals("n78004349", extractComparisonPart(LCCN, qualifier));
    assertEquals("n78004349", extractComparisonPart("n 78004349", qualifier));
  }

  @Test
  void shouldStripNonAsciiDigits_WhenExtractingNumericsOnly() {
    Qualifier qualifier = new Qualifier().withComparisonPart(NUMERICS_ONLY);

    assertEquals("78004349", extractComparisonPart("n १२ 78004349 ", qualifier));
    assertEquals("", extractComparisonPart("१२३", qualifier));
  }

  @Test
  void shouldStripOtherNumbers_WhenExtractingAlphanumericsOnly() {
    // Postgres alnum keeps letters and Nd/Nl but drops the Unicode "other number" category,
    // so fractions, superscripts and circled numerals must be stripped here too
    Qualifier qualifier = new Qualifier().withComparisonPart(ALPHANUMERICS_ONLY);

    assertEquals("n78004349", extractComparisonPart("n ½ 78004349 ", qualifier));
    assertEquals("vol2", extractComparisonPart("vol. ² 2", qualifier));
    // letters and decimal/letter numbers survive on both sides
    assertEquals("нet汉字éÓ१Ⅻ", extractComparisonPart("нet 汉字 éÓ-१ Ⅻ", qualifier));
  }

  @Test
  void shouldReturnOriginalValue_WhenComparisonPartIsNotSpecified() {
    Qualifier qualifier = new Qualifier()
      .withQualifierType(CONTAINS)
      .withQualifierValue("78004349");

    assertEquals(LCCN, extractComparisonPart(LCCN, qualifier));
    assertEquals(LCCN, extractComparisonPart(LCCN, null));
    assertNull(extractComparisonPart(null, qualifier));
  }
}
