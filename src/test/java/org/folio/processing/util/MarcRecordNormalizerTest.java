package org.folio.processing.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.marc4j.marc.DataField;
import org.marc4j.marc.MarcFactory;
import org.marc4j.marc.Record;

@RunWith(JUnit4.class)
class MarcRecordNormalizerTest {

  private static final String TAG_035 = "035";
  private static final String OCLC_PREFIX = "(OCoLC)";
  private static final String OCLC_12345 = OCLC_PREFIX + "12345";
  private static final String OCLC_0012345 = OCLC_PREFIX + "0012345";
  private static final String OCLC_11111 = OCLC_PREFIX + "11111";
  private static final String OCLC_22222 = OCLC_PREFIX + "22222";
  private static final String OCLC_99999 = OCLC_PREFIX + "99999";
  private static final String OCLC_123456 = OCLC_PREFIX + "123456";
  private static final String OCLC_987654 = OCLC_PREFIX + "987654";
  private static final String OCLC_987654321 = OCLC_PREFIX + "987654321";
  private static final String OCLC_1234567 = OCLC_PREFIX + "1234567";
  private static final String OCLC_123456ABC = OCLC_PREFIX + "123456abc";
  private static final String OCLC_TFE123 = OCLC_PREFIX + "tfe123";
  private static final String OCLC_NO_DIGITS = OCLC_PREFIX + "nodigitshere";
  private static final String OCLC_OCM_00123456 = OCLC_PREFIX + "ocm00123456";
  private static final String OCLC_OCM_987654321 = OCLC_PREFIX + "ocm987654321";
  private static final String OCLC_OCM_00123456_ABC = OCLC_PREFIX + "ocm00123456abc";
  private static final String OCLC_OCM_00099999 = OCLC_PREFIX + "ocm00099999";
  private static final String OCLC_OCN_0000987654 = OCLC_PREFIX + "ocn0000987654";
  private static final String OCLC_ON_001234567 = OCLC_PREFIX + "on001234567";
  private static final String OCLC_TFE_00123 = OCLC_PREFIX + "tfe00123";
  private static final String OCLC_AB2C_00456 = OCLC_PREFIX + "ab2c00456";
  private static final String OCLC_OCM_DOT_00123456 = OCLC_PREFIX + "ocm.00123456";
  private static final String OCLC_OCN_SPACE_987654 = OCLC_PREFIX + " ocn 00987654";
  private static final MarcFactory FACTORY = MarcFactory.newInstance();

  // ── Single-field formatting ──────────────────────────────────────────────────

  @ParameterizedTest(name = "{0}")
  @MethodSource("singleFieldFormattingParameters")
  void normalize035Field_singleFieldFormatting(String testName, String input, String expected) {
    var marcRecord = recordWith035(input);
    MarcRecordNormalizer.normalize035Field(marcRecord);
    assertEquals(expected, firstSubfieldData(marcRecord));
  }

  private static Stream<Arguments> singleFieldFormattingParameters() {
    return Stream.of(
      Arguments.of("non-OCLC prefix unchanged", "(NLC)123456", "(NLC)123456"),
      Arguments.of("blank data unchanged", "   ", "   "),
      Arguments.of("ocm prefix with leading zeros stripped", OCLC_OCM_00123456, OCLC_123456),
      Arguments.of("ocm prefix no leading zeros stripped", OCLC_OCM_987654321, OCLC_987654321),
      Arguments.of("ocn prefix with leading zeros stripped", OCLC_OCN_0000987654, OCLC_987654),
      Arguments.of("on prefix with leading zeros stripped", OCLC_ON_001234567, OCLC_1234567),
      Arguments.of("plain OCLC number leading zeros removed", OCLC_0012345, OCLC_12345),
      Arguments.of("plain OCLC number no leading zeros unchanged", OCLC_12345, OCLC_12345),
      Arguments.of("alphabetic prefix digits removed zeros stripped", OCLC_TFE_00123, OCLC_TFE123),
      Arguments.of("mixed alphabetic prefix digits unchanged", OCLC_AB2C_00456, OCLC_AB2C_00456),
      Arguments.of("dots removed before formatting", OCLC_OCM_DOT_00123456, OCLC_123456),
      Arguments.of("whitespace removed before formatting", OCLC_OCN_SPACE_987654, OCLC_987654),
      Arguments.of("leading whitespace before OCLC prefix trimmed", "  " + OCLC_OCM_00123456, OCLC_123456),
      Arguments.of("numeric part with trailing chars preserved", OCLC_OCM_00123456_ABC, OCLC_123456ABC),
      Arguments.of("no matching OCLC pattern subfield unchanged", OCLC_NO_DIGITS, OCLC_NO_DIGITS)
    );
  }

  // ── Deduplication ────────────────────────────────────────────────────────────
  //
  // Covers: no-op when there are no OCLC subfields, duplicate removal between
  // separate fields, duplicate removal within a single field, and the case where
  // values are equal across fields but not identical in string form before
  // normalization.

  @ParameterizedTest(name = "{0}")
  @MethodSource("deduplicationParameters")
  void normalize035Field_deduplication(String testName, Record input,
                                        int expectedFieldCount, String expectedFirstValue) {
    MarcRecordNormalizer.normalize035Field(input);
    var remaining = input.getVariableFields(TAG_035);
    assertEquals(expectedFieldCount, remaining.size());
    if (expectedFirstValue != null) {
      assertEquals(expectedFirstValue, ((DataField) remaining.getFirst()).getSubfield('a').getData());
    }
  }

  private static Stream<Arguments> deduplicationParameters() {
    return Stream.of(
      Arguments.of("no fields in record — record unchanged",
        FACTORY.newRecord(), 0, null),
      Arguments.of("non-035 field only — record unchanged",
        recordWithNon035Field(), 0, null),
      Arguments.of("two standalones normalizing to same value — one removed",
        recordWithTwoDuplicate035Fields(), 1, OCLC_12345),
      Arguments.of("internal duplicate subfield removed, field kept",
        recordWithInternalDuplicateSubfields(), 1, OCLC_12345),
      Arguments.of("standalone field removed entirely when sole duplicate",
        recordWithDuplicateStandaloneField(), 1, OCLC_99999),
      Arguments.of("two standalones with different values — both kept",
        recordWithTwoDifferentValues(), 2, null)
    );
  }

  // ── Multi-subfield preservation ───────────────────────────────────────────────
  //
  // When a 035 field carries $a plus other subfields ($z, etc.) and a standalone
  // 035 field contains only a duplicate of the $a, the multi-subfield field must
  // survive intact.  The standalone must be removed rather than stripping $a from
  // the richer field (which would leave an orphaned $z-only 035).

  @ParameterizedTest(name = "{0}")
  @MethodSource("multiSubfieldPreservationParameters")
  void normalize035Field_multiSubfieldPreservation(String testName, String inputValue,
                                                    String expectedValue) {
    var multiField = FACTORY.newDataField(TAG_035, ' ', ' ');
    multiField.addSubfield(FACTORY.newSubfield('a', inputValue));
    multiField.addSubfield(FACTORY.newSubfield('z', "(OCoLC)1079294651"));
    multiField.addSubfield(FACTORY.newSubfield('z', "(OCoLC)1130250129"));
    var standalone = FACTORY.newDataField(TAG_035, ' ', ' ');
    standalone.addSubfield(FACTORY.newSubfield('a', "(OCoLC)1299112"));
    var marcRecord = FACTORY.newRecord();
    marcRecord.addVariableField(multiField);
    marcRecord.addVariableField(standalone);

    MarcRecordNormalizer.normalize035Field(marcRecord);

    var remaining = marcRecord.getVariableFields(TAG_035);
    assertEquals(1, remaining.size());
    var df = (DataField) remaining.getFirst();
    assertEquals(expectedValue, df.getSubfield('a').getData());
    assertEquals(2, df.getSubfields('z').size());
    assertEquals("(OCoLC)1079294651", df.getSubfields('z').get(0).getData());
    assertEquals("(OCoLC)1130250129", df.getSubfields('z').get(1).getData());
  }

  private static Stream<Arguments> multiSubfieldPreservationParameters() {
    return Stream.of(
      Arguments.of("already-normalized $a — standalone duplicate removed",
        "(OCoLC)1299112", "(OCoLC)1299112"),
      Arguments.of("non-normalized $a — normalized after dedup, standalone removed",
        "(OCoLC)ocm1299112", "(OCoLC)1299112")
    );
  }

  // ── Individual tests (unique assertion shapes) ────────────────────────────────

  @Test
  void normalize035Field_nullRecord_doesNotThrow() {
    assertDoesNotThrow(() -> MarcRecordNormalizer.normalize035Field(null));
  }

  @Test
  void normalize035Field_mixedSubfieldsInSameField_onlyOclcSubfieldFormatted() {
    var field = FACTORY.newDataField(TAG_035, ' ', ' ');
    field.addSubfield(FACTORY.newSubfield('a', OCLC_OCM_00123456));
    field.addSubfield(FACTORY.newSubfield('z', "(NLC)999"));
    var marcRecord = FACTORY.newRecord();
    marcRecord.addVariableField(field);

    MarcRecordNormalizer.normalize035Field(marcRecord);

    var df = (DataField) marcRecord.getVariableFields(TAG_035).getFirst();
    assertEquals(OCLC_123456, df.getSubfield('a').getData());
    assertEquals("(NLC)999", df.getSubfield('z').getData());
  }

  @Test
  void normalize035Field_multipleOclcSubfieldKinds_onlyDuplicatesRemoved() {
    var field1 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field1.addSubfield(FACTORY.newSubfield('a', "(OCoLC)64758"));
    var field2 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field2.addSubfield(FACTORY.newSubfield('a', "(OCoLC)ocm000064758"));
    field2.addSubfield(FACTORY.newSubfield('k', "(OCoLC)976939443"));
    field2.addSubfield(FACTORY.newSubfield('k', "(OCoLC)1001261435"));
    field2.addSubfield(FACTORY.newSubfield('k', "(OCoLC)120194933"));
    var marcRecord = FACTORY.newRecord();
    marcRecord.addVariableField(field1);
    marcRecord.addVariableField(field2);

    MarcRecordNormalizer.normalize035Field(marcRecord);

    var updatedField = (DataField) marcRecord.getVariableField(TAG_035);
    assertNotNull(updatedField);
    assertEquals(4, updatedField.getSubfields().size());
    assertEquals("(OCoLC)64758", updatedField.getSubfield('a').getData());
    var subfieldsK = updatedField.getSubfields('k');
    assertEquals(3, subfieldsK.size());
    assertEquals("(OCoLC)976939443", subfieldsK.get(0).getData());
    assertEquals("(OCoLC)1001261435", subfieldsK.get(1).getData());
    assertEquals("(OCoLC)120194933", subfieldsK.get(2).getData());
  }

  @Test
  void normalize035Field_fieldWithInternalDuplicatesAndBsubfield_innerSubfieldsAreStripped() {
    // 035[$a(OCoLC)ocn0001234, $a(OCoLC)ocn1234, $b(OCoLC)ocn1234] — internal $a duplicates + $b
    // 035[$a(OCoLC)ocm1234]  — standalone, same OCLC number
    // 035[$a(OCoLC)ocn00098765] — standalone, different number
    // 035[$a(OCoLC)ocn0001234]  — standalone, same (the keeper)
    var complexField = FACTORY.newDataField(TAG_035, ' ', ' ');
    complexField.addSubfield(FACTORY.newSubfield('a', "(OCoLC)ocn0001234"));
    complexField.addSubfield(FACTORY.newSubfield('a', "(OCoLC)ocn1234"));
    complexField.addSubfield(FACTORY.newSubfield('b', "(OCoLC)ocn1234"));
    var standalone1 = FACTORY.newDataField(TAG_035, ' ', ' ');
    standalone1.addSubfield(FACTORY.newSubfield('a', "(OCoLC)ocm1234"));
    var standalone2 = FACTORY.newDataField(TAG_035, ' ', ' ');
    standalone2.addSubfield(FACTORY.newSubfield('a', "(OCoLC)ocn00098765"));
    var standalone3 = FACTORY.newDataField(TAG_035, ' ', ' ');
    standalone3.addSubfield(FACTORY.newSubfield('a', "(OCoLC)ocn0001234"));
    var marcRecord = FACTORY.newRecord();
    marcRecord.addVariableField(complexField);
    marcRecord.addVariableField(standalone1);
    marcRecord.addVariableField(standalone2);
    marcRecord.addVariableField(standalone3);

    MarcRecordNormalizer.normalize035Field(marcRecord);

    var remaining = marcRecord.getVariableFields(TAG_035);
    // complexField stripped to [$b(OCoLC)1234], standalone2 [$a(OCoLC)98765], one [$a(OCoLC)1234]
    assertEquals(3, remaining.size());
    var fieldWithBsubfield = remaining.stream()
      .map(f -> (DataField) f)
      .filter(f -> f.getSubfield('b') != null && f.getSubfield('a') == null)
      .findFirst();
    assertTrue("Expected a $b-only 035 field", fieldWithBsubfield.isPresent());
    assertEquals("(OCoLC)1234", fieldWithBsubfield.get().getSubfield('b').getData());
    var fieldsWithA = remaining.stream()
      .map(f -> (DataField) f)
      .filter(f -> f.getSubfield('a') != null)
      .toList();
    assertEquals(2, fieldsWithA.size());
    assertTrue(fieldsWithA.stream().anyMatch(f -> "(OCoLC)98765".equals(f.getSubfield('a').getData())));
    assertTrue(fieldsWithA.stream().anyMatch(f -> "(OCoLC)1234".equals(f.getSubfield('a').getData())));
  }

  // ── Private helpers ───────────────────────────────────────────────────────────

  private Record recordWith035(String subfieldValue) {
    var field = FACTORY.newDataField(TAG_035, ' ', ' ');
    field.addSubfield(FACTORY.newSubfield('a', subfieldValue));
    var marcRecord = FACTORY.newRecord();
    marcRecord.addVariableField(field);
    return marcRecord;
  }

  private String firstSubfieldData(Record marcRecord) {
    var fields = marcRecord.getVariableFields(TAG_035);
    if (fields.isEmpty()) {
      return null;
    }
    var df = (DataField) fields.getFirst();
    var sf = df.getSubfield('a');
    return sf == null ? null : sf.getData();
  }

  private static Record recordWithNon035Field() {
    var field = FACTORY.newDataField("100", ' ', ' ');
    field.addSubfield(FACTORY.newSubfield('a', OCLC_12345));
    var marcRecord = FACTORY.newRecord();
    marcRecord.addVariableField(field);
    return marcRecord;
  }

  private static Record recordWithTwoDuplicate035Fields() {
    var field1 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field1.addSubfield(FACTORY.newSubfield('a', OCLC_0012345));
    var field2 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field2.addSubfield(FACTORY.newSubfield('a', OCLC_12345));
    var marcRecord = FACTORY.newRecord();
    marcRecord.addVariableField(field1);
    marcRecord.addVariableField(field2);
    return marcRecord;
  }

  private static Record recordWithInternalDuplicateSubfields() {
    var field = FACTORY.newDataField(TAG_035, ' ', ' ');
    field.addSubfield(FACTORY.newSubfield('a', OCLC_12345));
    field.addSubfield(FACTORY.newSubfield('a', OCLC_0012345));
    var marcRecord = FACTORY.newRecord();
    marcRecord.addVariableField(field);
    return marcRecord;
  }

  private static Record recordWithDuplicateStandaloneField() {
    var field1 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field1.addSubfield(FACTORY.newSubfield('a', OCLC_OCM_00099999));
    var field2 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field2.addSubfield(FACTORY.newSubfield('a', OCLC_99999));
    var marcRecord = FACTORY.newRecord();
    marcRecord.addVariableField(field1);
    marcRecord.addVariableField(field2);
    return marcRecord;
  }

  private static Record recordWithTwoDifferentValues() {
    var field1 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field1.addSubfield(FACTORY.newSubfield('a', OCLC_11111));
    var field2 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field2.addSubfield(FACTORY.newSubfield('a', OCLC_22222));
    var marcRecord = FACTORY.newRecord();
    marcRecord.addVariableField(field1);
    marcRecord.addVariableField(field2);
    return marcRecord;
  }
}
