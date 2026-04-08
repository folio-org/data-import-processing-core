package org.folio.processing.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
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

  @ParameterizedTest(name = "{0}")
  @MethodSource("singleFieldFormattingParameters")
  void normalize035Field_singleFieldFormatting(String testName, String input, String expected) {
    var marcRecord = recordWith035(input);
    MarcRecordNormalizer.normalize035Field(marcRecord);
    assertEquals(expected, firstSubfieldData(marcRecord));
  }

  @Test
  void normalize035Field_nullRecord_doesNotThrow() {
    assertDoesNotThrow(() -> MarcRecordNormalizer.normalize035Field(null));
  }

  @Test
  void normalize035Field_noFields_recordUnchanged() {
    var marcRecord = FACTORY.newRecord();
    MarcRecordNormalizer.normalize035Field(marcRecord);
    assertTrue(marcRecord.getVariableFields(TAG_035).isEmpty());
  }

  @Test
  void normalize035Field_no035Fields_recordUnchanged() {
    var marcRecord = FACTORY.newRecord();
    var field = FACTORY.newDataField("100", ' ', ' ');
    field.addSubfield(FACTORY.newSubfield('a', OCLC_12345));
    marcRecord.addVariableField(field);

    MarcRecordNormalizer.normalize035Field(marcRecord);

    assertTrue(marcRecord.getVariableFields(TAG_035).isEmpty());
  }

  @Test
  void normalize035Field_twoDuplicate035Fields_oneRemoved() {
    var marcRecord = FACTORY.newRecord();

    var field1 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field1.addSubfield(FACTORY.newSubfield('a', OCLC_0012345));

    var field2 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field2.addSubfield(FACTORY.newSubfield('a', OCLC_12345));

    marcRecord.addVariableField(field1);
    marcRecord.addVariableField(field2);

    MarcRecordNormalizer.normalize035Field(marcRecord);

    var remaining = marcRecord.getVariableFields(TAG_035);
    assertEquals(1, remaining.size());
    assertEquals(OCLC_12345, ((DataField) remaining.getFirst()).getSubfield('a').getData());
  }

  @Test
  void normalize035Field_duplicateSubfieldInSameField_subfieldRemovedFieldKept() {
    var marcRecord = FACTORY.newRecord();
    var field = FACTORY.newDataField(TAG_035, ' ', ' ');
    field.addSubfield(FACTORY.newSubfield('a', OCLC_12345));
    field.addSubfield(FACTORY.newSubfield('a', OCLC_0012345));
    marcRecord.addVariableField(field);

    MarcRecordNormalizer.normalize035Field(marcRecord);

    var remaining = marcRecord.getVariableFields(TAG_035);
    assertEquals(1, remaining.size());
    var subfields = ((DataField) remaining.getFirst()).getSubfields('a');
    assertEquals(1, subfields.size());
    assertEquals(OCLC_12345, subfields.getFirst().getData());
  }

  @Test
  void normalize035Field_duplicateSubfieldOnlySubfieldInField_wholeFieldRemoved() {
    var marcRecord = FACTORY.newRecord();

    var field1 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field1.addSubfield(FACTORY.newSubfield('a', OCLC_OCM_00099999));

    var field2 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field2.addSubfield(FACTORY.newSubfield('a', OCLC_99999));

    marcRecord.addVariableField(field1);
    marcRecord.addVariableField(field2);

    MarcRecordNormalizer.normalize035Field(marcRecord);

    var remaining = marcRecord.getVariableFields(TAG_035);
    assertEquals(1, remaining.size());
  }

  @Test
  void normalize035Field_twoDifferentOclcValues_bothKept() {
    var marcRecord = FACTORY.newRecord();

    var field1 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field1.addSubfield(FACTORY.newSubfield('a', OCLC_11111));

    var field2 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field2.addSubfield(FACTORY.newSubfield('a', OCLC_22222));

    marcRecord.addVariableField(field1);
    marcRecord.addVariableField(field2);

    MarcRecordNormalizer.normalize035Field(marcRecord);

    var remaining = marcRecord.getVariableFields(TAG_035);
    assertEquals(2, remaining.size());
  }

  @Test
  void shouldHandleMultipleSubfieldsIn035() {
    var basicRecord = FACTORY.newRecord();

    var field1 = FACTORY.newDataField(TAG_035, ' ', ' ');
    field1.addSubfield(FACTORY.newSubfield('a', "(OCoLC)64758"));
    basicRecord.addVariableField(field1);

    var field2 = FACTORY.newDataField("035", ' ', ' ');
    field2.addSubfield(FACTORY.newSubfield('a', "(OCoLC)ocm000064758"));
    field2.addSubfield(FACTORY.newSubfield('k', "(OCoLC)976939443"));
    field2.addSubfield(FACTORY.newSubfield('k', "(OCoLC)1001261435"));
    field2.addSubfield(FACTORY.newSubfield('k', "(OCoLC)120194933"));
    basicRecord.addVariableField(field2);

    MarcRecordNormalizer.normalize035Field(basicRecord);

    var updatedField = (DataField) basicRecord.getVariableField("035");
    assertNotNull(updatedField);
    Assertions.assertEquals(4, updatedField.getSubfields().size());
    Assertions.assertEquals("(OCoLC)64758", updatedField.getSubfield('a').getData());
    var subfieldsK = updatedField.getSubfields('k');
    Assertions.assertEquals(3, subfieldsK.size());
    Assertions.assertEquals("(OCoLC)976939443", subfieldsK.get(0).getData());
    Assertions.assertEquals("(OCoLC)1001261435", subfieldsK.get(1).getData());
    Assertions.assertEquals("(OCoLC)120194933", subfieldsK.get(2).getData());
  }

  @Test
  void normalize035Field_mixedSubfieldsInSameField_onlyOclcSubfieldFormatted() {
    var marcRecord = FACTORY.newRecord();
    var field = FACTORY.newDataField(TAG_035, ' ', ' ');
    field.addSubfield(FACTORY.newSubfield('a', OCLC_OCM_00123456));
    field.addSubfield(FACTORY.newSubfield('z', "(NLC)999"));
    marcRecord.addVariableField(field);

    MarcRecordNormalizer.normalize035Field(marcRecord);

    var df = (DataField) marcRecord.getVariableFields(TAG_035).getFirst();
    assertEquals(OCLC_123456, df.getSubfield('a').getData());
    assertEquals("(NLC)999", df.getSubfield('z').getData());
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

  private Record recordWith035(String subfieldAValue) {
    var field = FACTORY.newDataField(TAG_035, ' ', ' ');
    field.addSubfield(FACTORY.newSubfield('a', subfieldAValue));
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
}
