package org.folio.processing.matching.reader.util;

import static org.folio.processing.value.Value.ValueType.MISSING;
import static org.folio.processing.value.Value.ValueType.STRING;

import java.util.HashMap;

import org.folio.DataImportEventPayload;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import junit.framework.TestCase;

@RunWith(JUnit4.class)
public class MatchIdProcessorUtilTest extends TestCase {

  private static final String MAPPING_PARAMS = "MAPPING_PARAMS";
  private static final String RELATIONS = "MATCHING_PARAMETERS_RELATIONS";
  private static final String MATCHING_RELATIONS = "{\"item.statisticalCodeIds[]\":\"statisticalCode\",\"instance.classifications[].classificationTypeId\":\"classificationTypes\",\"instance.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"item.permanentLoanTypeId\":\"loantypes\",\"holdingsrecord.temporaryLocationId\":\"locations\",\"holdingsrecord.statisticalCodeIds[]\":\"statisticalCode\",\"instance.statusId\":\"instanceStatuses\",\"instance.natureOfContentTermIds\":\"natureOfContentTerms\",\"item.notes[].itemNoteTypeId\":\"itemNoteTypes\",\"holdingsrecord.permanentLocationId\":\"locations\",\"instance.alternativeTitles[].alternativeTitleTypeId\":\"alternativeTitleTypes\",\"holdingsrecord.illPolicyId\":\"illPolicies\",\"item.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"instance.identifiers[].identifierTypeId\":\"identifierTypes\",\"holdingsrecord.holdingsTypeId\":\"holdingsTypes\",\"item.permanentLocationId\":\"locations\",\"instance.modeOfIssuanceId\":\"issuanceModes\",\"item.itemLevelCallNumberTypeId\":\"callNumberTypes\",\"instance.notes[].instanceNoteTypeId\":\"instanceNoteTypes\",\"instance.instanceFormatIds\":\"instanceFormats\",\"holdingsrecord.callNumberTypeId\":\"callNumberTypes\",\"holdingsrecord.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"instance.instanceTypeId\":\"instanceTypes\",\"instance.statisticalCodeIds[]\":\"statisticalCode\",\"instancerelationship.instanceRelationshipTypeId\":\"instanceRelationshipTypes\",\"item.temporaryLoanTypeId\":\"loantypes\",\"item.temporaryLocationId\":\"locations\",\"item.materialTypeId\":\"materialTypes\",\"holdingsrecord.notes[].holdingsNoteTypeId\":\"holdingsNoteTypes\",\"instance.contributors[].contributorNameTypeId\":\"contributorNameTypes\",\"item.itemDamagedStatusId\":\"itemDamageStatuses\",\"instance.contributors[].contributorTypeId\":\"contributorTypes\"}";

  private static final String LOCATIONS_PARAMS = "{\"initialized\":true,\"locations\":[{\"id\":\"53cf956f-c1df-410b-8bea-27f712cca7c0\",\"name\":\"Annex\",\"code\":\"KU/CC/DI/A\",\"isActive\":true,\"institutionId\":\"40ee00ca-a518-4b49-be01-0638d0a4ac57\",\"campusId\":\"62cf76b7-cca5-4d33-9217-edf42ce1a848\",\"libraryId\":\"5d78803e-ca04-4b4a-aeae-2c63b924518b\",\"primaryServicePoint\":\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\",\"servicePointIds\":[\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\"],\"servicePoints\":[],\"metadata\":{\"createdDate\":1592219257690,\"updatedDate\":1592219257690}},{\"id\":\"b241764c-1466-4e1d-a028-1a3684a5da87\",\"name\":\"Popular Reading Collection\",\"code\":\"KU/CC/DI/P\",\"isActive\":true,\"institutionId\":\"40ee00ca-a518-4b49-be01-0638d0a4ac57\",\"campusId\":\"62cf76b7-cca5-4d33-9217-edf42ce1a848\",\"libraryId\":\"5d78803e-ca04-4b4a-aeae-2c63b924518b\",\"primaryServicePoint\":\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\",\"servicePointIds\":[\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\"],\"servicePoints\":[],\"metadata\":{\"createdDate\":1592219257711,\"updatedDate\":1592219257711}}]}";
  private static final String ITEM_NOTE_TYPES_PARAMS = "{\"initialized\":true,\"itemNoteTypes\":[{\"id\":\"87c450be-2033-41fb-80ba-dd2409883681\",\"name\":\"Binding\",\"source\":\"folio\",\"metadata\":{\"createdDate\":1592219267545,\"updatedDate\":1592219267545}},{\"id\":\"8d0a5eca-25de-4391-81a9-236eeefdd20b\",\"name\":\"Note\",\"source\":\"folio\",\"metadata\":{\"createdDate\":1592219267556,\"updatedDate\":1592219267556}}]}";

  @Test
  public void shouldReadStringLocationValueAndReturnId() {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();

    HashMap<String, String> context = new HashMap<>();
    context.put(MAPPING_PARAMS, LOCATIONS_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);
    eventPayload.setContext(context);
    String textValue = "KU/CC/DI/A";

    //when
    Value result = MatchIdProcessorUtil.retrieveIdFromContext("holdingsrecord.permanentLocationId", eventPayload, StringValue.of(textValue));
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    String expectedId = "53cf956f-c1df-410b-8bea-27f712cca7c0";
    assertEquals(expectedId, result.getValue());
  }

  @Test
  public void shouldReadStringLocationValueAndReturnMissingIfCantFindId() {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();

    HashMap<String, String> context = new HashMap<>();
    context.put(MAPPING_PARAMS, "{\"initialized\":true,\"locations\":[{\"id\":\"53cf956f-c1df-410b-8bea-27f712cca7c0\",\"name\":\"NotMatchedValue\",\"code\":\"KU/CC/DI/M\"}]}");
    context.put(RELATIONS, MATCHING_RELATIONS);
    eventPayload.setContext(context);
    String textValue = "KU/CC/DI/A";

    //when
    Value result = MatchIdProcessorUtil.retrieveIdFromContext("holdingsrecord.permanentLocationId", eventPayload, StringValue.of(textValue));
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
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

    //when
    Value result = MatchIdProcessorUtil.retrieveIdFromContext("item.notes[].itemNoteTypeId", eventPayload, StringValue.of(textValue));
    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    String expectedId = "8d0a5eca-25de-4391-81a9-236eeefdd20b";
    assertEquals(expectedId, result.getValue());
  }

  @Test
  public void shouldReadStringValueAndReturnMissingIfCantFindId() {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();

    HashMap<String, String> context = new HashMap<>();
    context.put(MAPPING_PARAMS, "{\"initialized\":true,\"itemNoteTypes\":[{\"id\":\"53cf956f-c1df-410b-8bea-27f712cca7c0\",\"name\":\"NotMatchedValue\",\"code\":\"CODE\"}]}");
    context.put(RELATIONS, MATCHING_RELATIONS);
    eventPayload.setContext(context);
    String textValue = "InvalidValue";

    //when
    Value result = MatchIdProcessorUtil.retrieveIdFromContext("item.notes[].itemNoteTypeId", eventPayload, StringValue.of(textValue));
    //then
    assertNotNull(result);
    assertEquals(MISSING, result.getType());
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

    //when
    Value result = MatchIdProcessorUtil.retrieveIdFromContext("holdingsrecord.notExistsField", eventPayload, StringValue.of(textValue));

    //then
    assertNotNull(result);
    assertEquals(STRING, result.getType());
    assertEquals(textValue, result.getValue());
  }
}
