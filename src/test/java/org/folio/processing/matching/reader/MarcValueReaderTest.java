package org.folio.processing.matching.reader;

import org.apache.commons.lang.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.processing.TestUtil;
import org.folio.processing.matching.model.schemas.Field;
import org.folio.processing.matching.model.schemas.MatchDetail;
import org.folio.processing.matching.model.schemas.MatchExpression;
import org.folio.processing.matching.model.schemas.Qualifier;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.Value;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import static org.folio.processing.matching.model.schemas.MatchExpression.DataValueType.STATIC_VALUE;
import static org.folio.processing.matching.model.schemas.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.processing.matching.model.schemas.MatchProfile.IncomingRecordType.MARC;
import static org.folio.processing.matching.model.schemas.Qualifier.ComparisonPart.ALPHANUMERICS_ONLY;
import static org.folio.processing.matching.model.schemas.Qualifier.ComparisonPart.NUMERICS_ONLY;
import static org.folio.processing.matching.model.schemas.Qualifier.QualifierType.BEGINS_WITH;
import static org.folio.processing.matching.model.schemas.Qualifier.QualifierType.CONTAINS;
import static org.folio.processing.matching.model.schemas.Qualifier.QualifierType.ENDS_WITH;
import static org.folio.processing.value.Value.ValueType.LIST;
import static org.folio.processing.value.Value.ValueType.MISSING;
import static org.folio.processing.value.Value.ValueType.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class MarcValueReaderTest {

  private final static String MARC_RECORD_PATH = "src/test/resources/org.folio.processing/marcRecord.json";
  private static String MARC_RECORD;

  @BeforeClass
  public static void setUp() throws IOException {
    MARC_RECORD = TestUtil.readFileFromPath(MARC_RECORD_PATH);
  }

  @Test
  public void shouldRead_StringValue() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("001"),
          new Field().withLabel("indicator1").withValue(StringUtils.EMPTY),
          new Field().withLabel("indicator2").withValue(StringUtils.EMPTY),
          new Field().withLabel("recordSubfield").withValue(StringUtils.EMPTY)
        )));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals("ybp7406411", result.getValue());
  }

  @Test
  public void shouldRead_SubfieldValue_WithEmptyIndicators() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("250"),
          new Field().withLabel("indicator1").withValue(" "),
          new Field().withLabel("indicator2").withValue(" "),
          new Field().withLabel("recordSubfield").withValue("a")
        )));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals("2nd ed.", result.getValue());
  }

  @Test
  public void shouldRead_SubfieldValue_WithIndicators() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("050"),
          new Field().withLabel("indicator1").withValue(" "),
          new Field().withLabel("indicator2").withValue("4"),
          new Field().withLabel("recordSubfield").withValue("b")
        )));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals(".A43 2011", result.getValue());
  }

  @Test
  public void shouldReturn_MissingValue_IfNoSuchField() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("021"),
          new Field().withLabel("indicator1").withValue(StringUtils.EMPTY),
          new Field().withLabel("indicator2").withValue(StringUtils.EMPTY),
          new Field().withLabel("recordSubfield").withValue(StringUtils.EMPTY)
        )));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReturn_MissingValue_IfNoSuchField_WithIndicators() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("999"),
          new Field().withLabel("indicator1").withValue("f"),
          new Field().withLabel("indicator2").withValue("f"),
          new Field().withLabel("recordSubfield").withValue("i")
        )));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReturn_MissingValue_IfNoSuchSubfieldField() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("980"),
          new Field().withLabel("indicator1").withValue(" "),
          new Field().withLabel("indicator2").withValue(" "),
          new Field().withLabel("recordSubfield").withValue("i")
        )));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReturn_ListValue_IfMultipleFields() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("020"),
          new Field().withLabel("indicator1").withValue(" "),
          new Field().withLabel("indicator2").withValue(" "),
          new Field().withLabel("recordSubfield").withValue("a")
        )));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(LIST, result.getType());
    ListValue listValue = (ListValue) result;
    assertEquals(2, listValue.getValue().size());
    assertTrue(listValue.getValue().contains("2940447241 (electronic bk.)"));
    assertTrue(listValue.getValue().contains("9782940447244 (electronic bk.)"));
  }

  @Test
  public void shouldReturn_MissingValue_IfOtherDataValueType() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(STATIC_VALUE));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReturn_MissingValue_IfEmptyRecord() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), StringUtils.EMPTY);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("020"),
          new Field().withLabel("indicator1").withValue(" "),
          new Field().withLabel("indicator2").withValue(" "),
          new Field().withLabel("recordSubfield").withValue("a")
        )));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReturn_MissingValue_IfDoNotMatchIndicator_1() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("100"),
          new Field().withLabel("indicator1").withValue("1"),
          new Field().withLabel("indicator2").withValue("2"),
          new Field().withLabel("recordSubfield").withValue("a")
        )));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReturn_MissingValue_IfDoNotMatchIndicator_2() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("245"),
          new Field().withLabel("indicator1").withValue("1"),
          new Field().withLabel("indicator2").withValue("2"),
          new Field().withLabel("recordSubfield").withValue("a")
        )));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReturn_ListValue_IfMultipleSubFields() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("776"),
          new Field().withLabel("indicator1").withValue(" "),
          new Field().withLabel("indicator2").withValue(" "),
          new Field().withLabel("recordSubfield").withValue("z")
        )));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(LIST, result.getType());
    ListValue listValue = (ListValue) result;
    assertEquals(2, listValue.getValue().size());
    assertTrue(listValue.getValue().contains("9782940411764"));
    assertTrue(listValue.getValue().contains("294041176X"));
  }

  @Test
  public void shouldReturn_StringValue_IfMultipleSubFields_FilterWithBeginsWithQualifier() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("776"),
          new Field().withLabel("indicator1").withValue(" "),
          new Field().withLabel("indicator2").withValue(" "),
          new Field().withLabel("recordSubfield").withValue("z")
        ))
        .withQualifier(new Qualifier()
          .withQualifierType(BEGINS_WITH)
          .withQualifierValue("978")));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals("9782940411764", result.getValue());
  }

  @Test
  public void shouldReturn_StringValue_IfMultipleSubFields_FilterWithEndsWithQualifier() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("776"),
          new Field().withLabel("indicator1").withValue(" "),
          new Field().withLabel("indicator2").withValue(" "),
          new Field().withLabel("recordSubfield").withValue("z")
        ))
        .withQualifier(new Qualifier()
          .withQualifierType(ENDS_WITH)
          .withQualifierValue("X")));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals("294041176X", result.getValue());
  }

  @Test
  public void shouldReturn_ListValue_IfMultipleFields_FilterWithContainsQualifier() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("020"),
          new Field().withLabel("indicator1").withValue(" "),
          new Field().withLabel("indicator2").withValue(" "),
          new Field().withLabel("recordSubfield").withValue("a")
        ))
        .withQualifier(new Qualifier()
          .withQualifierType(CONTAINS)
          .withQualifierValue("(electronic bk.)")));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(LIST, result.getType());
    ListValue listValue = (ListValue) result;
    assertEquals(2, listValue.getValue().size());
    assertTrue(listValue.getValue().contains("2940447241 (electronic bk.)"));
    assertTrue(listValue.getValue().contains("9782940447244 (electronic bk.)"));
  }

  @Test
  public void shouldReturn_StringValue_IfMultipleSubFields_WithComparisonPart() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("776"),
          new Field().withLabel("indicator1").withValue(" "),
          new Field().withLabel("indicator2").withValue(" "),
          new Field().withLabel("recordSubfield").withValue("z")
        ))
        .withQualifier(new Qualifier()
          .withQualifierType(ENDS_WITH)
          .withQualifierValue("X")
          .withComparisonPart(NUMERICS_ONLY)));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals("294041176", result.getValue());
  }

  @Test
  public void shouldReturn_StringValue_NumericOnly() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("001"),
          new Field().withLabel("indicator1").withValue(StringUtils.EMPTY),
          new Field().withLabel("indicator2").withValue(StringUtils.EMPTY),
          new Field().withLabel("recordSubfield").withValue(StringUtils.EMPTY)
        ))
        .withQualifier(new Qualifier()
          .withComparisonPart(NUMERICS_ONLY)));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals("7406411", result.getValue());
  }

  @Test
  public void shouldReturn_StringValue_AlphaNumericOnly() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("008"),
          new Field().withLabel("indicator1").withValue(StringUtils.EMPTY),
          new Field().withLabel("indicator2").withValue(StringUtils.EMPTY),
          new Field().withLabel("recordSubfield").withValue(StringUtils.EMPTY)
        ))
        .withQualifier(new Qualifier()
          .withComparisonPart(ALPHANUMERICS_ONLY)));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals("120329s2011szaob0010engd", result.getValue());
  }

  @Test
  public void shouldReturn_StringValue_AlphaNumericsOnly() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("006"),
          new Field().withLabel("indicator1").withValue(StringUtils.EMPTY),
          new Field().withLabel("indicator2").withValue(StringUtils.EMPTY),
          new Field().withLabel("recordSubfield").withValue(StringUtils.EMPTY)
        ))
        .withQualifier(new Qualifier()
          .withComparisonPart(ALPHANUMERICS_ONLY)));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals("md", result.getValue());
  }

  @Test
  public void shouldReturn_StringValue_AlphaNumerics() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("999"),
          new Field().withLabel("indicator1").withValue(" "),
          new Field().withLabel("indicator2").withValue(" "),
          new Field().withLabel("recordSubfield").withValue("c")
        ))
        .withQualifier(new Qualifier()
          .withComparisonPart(ALPHANUMERICS_ONLY)));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals("nl78netнэтъюююйцукbролл1234汉字éÓ", result.getValue());
  }

  @Test
  public void shouldReturn_StringValue_Numerics() {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC.value(), MARC_RECORD);
    eventContext.setContext(context);
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("999"),
          new Field().withLabel("indicator1").withValue(" "),
          new Field().withLabel("indicator2").withValue(" "),
          new Field().withLabel("recordSubfield").withValue("c")
        ))
        .withQualifier(new Qualifier()
          .withComparisonPart(NUMERICS_ONLY)));
    MatchValueReader reader = new MarcValueReaderImpl();
    //when
    Value result = reader.read(eventContext, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals("781234", result.getValue());
  }

}
