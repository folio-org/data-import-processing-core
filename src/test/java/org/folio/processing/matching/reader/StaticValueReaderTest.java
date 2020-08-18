package org.folio.processing.matching.reader;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.processing.value.DateValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.StaticValueDetails;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.folio.processing.value.Value.ValueType.DATE;
import static org.folio.processing.value.Value.ValueType.MISSING;
import static org.folio.processing.value.Value.ValueType.STRING;
import static org.folio.rest.jaxrs.model.EntityType.STATIC_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnit4.class)
public class StaticValueReaderTest {

  private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd";

  @Test
  public void shouldReadStringValue() {
    // given
    String textValue = "KU/CC/DI/M";
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.TEXT)
          .withText(textValue)));
    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals(textValue, result.getValue());
  }

  @Test
  public void shouldReturnMissingValueIfTextIsNull() {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.TEXT)
          .withText(null)));
    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReadNumberAsStringValue() {
    // given
    String numberValue = "42";
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.NUMBER)
          .withNumber(numberValue)));
    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals(numberValue, result.getValue());
  }

  @Test
  public void shouldReturnMissingValueIfNumberIsNull() {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.NUMBER)
          .withNumber(null)));
    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReadDateValue() {
    // given
    Date dateValue = new Date();
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.EXACT_DATE)
          .withExactDate(dateValue)));
    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(DATE, result.getType());
    DateValue resultValue = (DateValue) result;
    assertEquals(new SimpleDateFormat(DATE_FORMAT_PATTERN).format(dateValue), resultValue.getFromDate());
    assertEquals(new SimpleDateFormat(DATE_FORMAT_PATTERN).format(dateValue), resultValue.getToDate());
  }

  @Test
  public void shouldReturnMissingValueIfDateIsNull() {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.EXACT_DATE)
          .withExactDate(null)));
    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReadDateRangeValue() throws ParseException {
    // given
    SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT_PATTERN);
    Date fromDate = df.parse("2020-04-01");
    Date toDate = df.parse("2020-04-30");
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.DATE_RANGE)
          .withFromDate(fromDate)
          .withToDate(toDate)));
    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(DATE, result.getType());
    DateValue resultValue = (DateValue) result;
    assertEquals(df.format(fromDate), resultValue.getFromDate());
    assertEquals(df.format(toDate), resultValue.getToDate());
  }

  @Test
  public void shouldReturnMissingValueIfFromDateIsNull() {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.DATE_RANGE)
          .withFromDate(null)
          .withToDate(new Date())));
    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReturnMissingValueIfToDateIsNull() {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.DATE_RANGE)
          .withFromDate(new Date())
          .withToDate(null)));
    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReturnMissingValueIfWrongDataValueType() {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.VALUE_FROM_RECORD)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.DATE_RANGE)
          .withFromDate(new Date())
          .withToDate(new Date())));
    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }

  @Test
  public void shouldReturnMissingValueIfNoStaticValueDetails() {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE));
    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
  }
}
