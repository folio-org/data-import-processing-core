package org.folio.processing.matching.reader;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.value.DateValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.StaticValueDetails;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.folio.processing.value.Value.ValueType.DATE;
import static org.folio.processing.value.Value.ValueType.MISSING;
import static org.folio.processing.value.Value.ValueType.STRING;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.EntityType.STATIC_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;

import io.vertx.core.json.JsonObject;

@RunWith(JUnit4.class)
public class StaticValueReaderTest {

  private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd";
  private static final String MAPPING_PARAMS = "MAPPING_PARAMS";
  private static final String RELATIONS = "MATCHING_PARAMETERS_RELATIONS";
  private static final String MATCHING_RELATIONS = "{\"matchingRelations\":{\"item.statisticalCodeIds[]\":\"statisticalCode\",\"instance.classifications[].classificationTypeId\":\"classificationTypes\",\"instance.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"item.permanentLoanTypeId\":\"loantypes\",\"holdingsrecord.temporaryLocationId\":\"locations\",\"holdingsrecord.statisticalCodeIds[]\":\"statisticalCode\",\"instance.statusId\":\"instanceStatuses\",\"instance.natureOfContentTermIds\":\"natureOfContentTerms\",\"item.notes[].itemNoteTypeId\":\"itemNoteTypes\",\"holdingsrecord.permanentLocationId\":\"locations\",\"instance.alternativeTitles[].alternativeTitleTypeId\":\"alternativeTitleTypes\",\"holdingsrecord.illPolicyId\":\"illPolicies\",\"item.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"instance.identifiers[].identifierTypeId\":\"identifierTypes\",\"holdingsrecord.holdingsTypeId\":\"holdingsTypes\",\"item.permanentLocationId\":\"locations\",\"instance.modeOfIssuanceId\":\"issuanceModes\",\"item.itemLevelCallNumberTypeId\":\"callNumberTypes\",\"instance.notes[].instanceNoteTypeId\":\"instanceNoteTypes\",\"instance.instanceFormatIds\":\"instanceFormats\",\"holdingsrecord.callNumberTypeId\":\"callNumberTypes\",\"holdingsrecord.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"instance.instanceTypeId\":\"instanceTypes\",\"instance.statisticalCodeIds[]\":\"statisticalCode\",\"instancerelationship.instanceRelationshipTypeId\":\"instanceRelationshipTypes\",\"item.temporaryLoanTypeId\":\"loantypes\",\"item.temporaryLocationId\":\"locations\",\"item.materialTypeId\":\"materialTypes\",\"holdingsrecord.notes[].holdingsNoteTypeId\":\"holdingsNoteTypes\",\"instance.contributors[].contributorNameTypeId\":\"contributorNameTypes\",\"item.itemDamagedStatusId\":\"itemDamageStatuses\",\"instance.contributors[].contributorTypeId\":\"contributorTypes\"}}";

  private static final String LOCATIONS_PARAMS = "{\"initialized\":true,\"locations\":[{\"id\":\"53cf956f-c1df-410b-8bea-27f712cca7c0\",\"name\":\"Annex\",\"code\":\"KU/CC/DI/A\",\"isActive\":true,\"institutionId\":\"40ee00ca-a518-4b49-be01-0638d0a4ac57\",\"campusId\":\"62cf76b7-cca5-4d33-9217-edf42ce1a848\",\"libraryId\":\"5d78803e-ca04-4b4a-aeae-2c63b924518b\",\"primaryServicePoint\":\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\",\"servicePointIds\":[\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\"],\"servicePoints\":[],\"metadata\":{\"createdDate\":1592219257690,\"updatedDate\":1592219257690}},{\"id\":\"b241764c-1466-4e1d-a028-1a3684a5da87\",\"name\":\"Popular Reading Collection\",\"code\":\"KU/CC/DI/P\",\"isActive\":true,\"institutionId\":\"40ee00ca-a518-4b49-be01-0638d0a4ac57\",\"campusId\":\"62cf76b7-cca5-4d33-9217-edf42ce1a848\",\"libraryId\":\"5d78803e-ca04-4b4a-aeae-2c63b924518b\",\"primaryServicePoint\":\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\",\"servicePointIds\":[\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\"],\"servicePoints\":[],\"metadata\":{\"createdDate\":1592219257711,\"updatedDate\":1592219257711}}]}";
  private static final String ITEM_NOTE_TYPES_PARAMS = "{\"initialized\":true,\"itemNoteTypes\":[{\"id\":\"87c450be-2033-41fb-80ba-dd2409883681\",\"name\":\"Binding\",\"source\":\"folio\",\"metadata\":{\"createdDate\":1592219267545,\"updatedDate\":1592219267545}},{\"id\":\"8d0a5eca-25de-4391-81a9-236eeefdd20b\",\"name\":\"Note\",\"source\":\"folio\",\"metadata\":{\"createdDate\":1592219267556,\"updatedDate\":1592219267556}}]}";

  @Test
  public void shouldReadStringLocationValueAndReturnId() {
    // given
    String textValue = "KU/CC/DI/A";
    DataImportEventPayload eventPayload = new DataImportEventPayload();

    HashMap<String, String> context = new HashMap<>();
    context.put(MAPPING_PARAMS, LOCATIONS_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);
    eventPayload.setContext(context);

    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.TEXT)
          .withText(textValue)))
      .withExistingMatchExpression(new MatchExpression().withFields(Lists.newArrayList(new Field().withLabel("field").withValue("holdingsrecord.permanentLocationId"))));

    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    String expectedId = "53cf956f-c1df-410b-8bea-27f712cca7c0";
    assertEquals(expectedId, result.getValue());
  }

  @Test
  public void shouldReadStringValueAndReturnId() {
    // given
    String textValue = "Note";
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MAPPING_PARAMS, ITEM_NOTE_TYPES_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);
    eventPayload.setContext(context);

    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.TEXT)
          .withText(textValue)))
      .withExistingMatchExpression(new MatchExpression().withFields(Lists.newArrayList(new Field().withLabel("field").withValue("item.notes[].itemNoteTypeId"))));

    MatchValueReader reader = new StaticValueReaderImpl();
    //when
    Value result = reader.read(eventPayload, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    String expectedId = "8d0a5eca-25de-4391-81a9-236eeefdd20b";
    assertEquals(expectedId, result.getValue());
  }


  @Test
  public void shouldReadStringSimpleValueAndReturnIt() {
    // given
    String textValue = "Some text value";
    DataImportEventPayload eventPayload = new DataImportEventPayload();

    HashMap<String, String> context = new HashMap<>();
    context.put(MAPPING_PARAMS, ITEM_NOTE_TYPES_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);
    eventPayload.setContext(context);

    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.TEXT)
          .withText(textValue)))
      .withExistingMatchExpression(new MatchExpression().withFields(Lists.newArrayList(new Field().withLabel("field").withValue("holdingsrecord.randomValue"))));

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

    DataImportEventPayload eventPayload = new DataImportEventPayload();

    HashMap<String, String> context = new HashMap<>();
    context.put(MAPPING_PARAMS, LOCATIONS_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);
    eventPayload.setContext(context);

    // given
    MatchDetail matchDetail = new MatchDetail()
      .withIncomingRecordType(STATIC_VALUE)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
        .withStaticValueDetails(new StaticValueDetails()
          .withStaticValueType(StaticValueDetails.StaticValueType.TEXT)
          .withText(null)))
      .withExistingMatchExpression(new MatchExpression().withFields(Lists.newArrayList(new Field().withLabel("field").withValue("holdingsrecord.permanentLocationId"))));
    ;
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
