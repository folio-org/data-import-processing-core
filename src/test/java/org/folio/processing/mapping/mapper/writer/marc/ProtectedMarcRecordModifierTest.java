package org.folio.processing.mapping.mapper.writer.marc;

import io.vertx.core.json.Json;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Data;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static io.vertx.core.json.jackson.DatabindCodec.mapper;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MappingDetail.MarcMappingOption.MODIFY;
import static org.folio.rest.jaxrs.model.MarcSubfield.Subaction.REPLACE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class ProtectedMarcRecordModifierTest {

  public static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private MarcRecordModifier marcRecordModifier = new MarcRecordModifier();

  @Test
  public void shouldProtectControlField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00097nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = parsedContent;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("001")
      .withData("*");

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(Collections.singletonList(marcFieldProtectionSetting));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("001")
        .withIndicator1(null)
        .withIndicator2(null)
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield(""))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Collections.singletonList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingParameters, mappingProfile);
    marcRecordModifier.modifyRecord(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldDeleteUnprotectedControlField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00097nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"002\":\"whatever\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00097nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("001")
      .withData("*");

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(Collections.singletonList(marcFieldProtectionSetting));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("002")
        .withIndicator1(null)
        .withIndicator2(null)
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield(""))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Collections.singletonList(mappingDetail)));

    //when
    marcRecordModifier.initialize(eventPayload, mappingParameters, mappingProfile);
    marcRecordModifier.modifyRecord(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldProtectDataField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00129nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    String expectedParsedContent = parsedContent;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("999")
      .withIndicator1("f")
      .withIndicator2("f")
      .withSubfield("*")
      .withData("*");

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(Collections.singletonList(marcFieldProtectionSetting));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("999")
        .withIndicator1("f")
        .withIndicator2("f")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("s")
          .withSubaction(REPLACE)
          .withData(new Data().withText("blabla")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Collections.singletonList(mappingDetail)));

    //when
    marcRecordModifier.initialize(eventPayload, mappingParameters, mappingProfile);
    marcRecordModifier.modifyRecord(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldDeleteUnprotectedField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00129nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}},{\"999\":{\"subfields\":[{\"a\":\"original\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00129nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("999")
      .withIndicator1("f")
      .withIndicator2("f")
      .withSubfield("*")
      .withData("*");

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(Collections.singletonList(marcFieldProtectionSetting));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("999")
        .withIndicator1(" ")
        .withIndicator2(" ")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("a"))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Collections.singletonList(mappingDetail)));

    //when
    marcRecordModifier.initialize(eventPayload, mappingParameters, mappingProfile);
    marcRecordModifier.modifyRecord(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldProtectAnyDataFieldWithSpecifiedSubfieldAndData() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00156nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\"0\",\"ind2\":\"4\"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    String expectedParsedContent = parsedContent;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("*")
      .withIndicator1("*")
      .withIndicator2("*")
      .withSubfield("a")
      .withData("electronic");

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(Collections.singletonList(marcFieldProtectionSetting));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("*"))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Collections.singletonList(mappingDetail)));

    //when
    marcRecordModifier.initialize(eventPayload, mappingParameters, mappingProfile);
    marcRecordModifier.modifyRecord(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldProtectDataFieldRegardlessItsIndicatorsAndSubfields() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00156nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"035\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\"0\",\"ind2\":\"4\"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    String expectedParsedContent = parsedContent;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("035")
      .withIndicator1("*")
      .withIndicator2("*")
      .withSubfield("*")
      .withData("*");

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(Collections.singletonList(marcFieldProtectionSetting));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(mappingParameters));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("035")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("*"))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Collections.singletonList(mappingDetail)));

    //when
    marcRecordModifier.initialize(eventPayload, mappingParameters, mappingProfile);
    marcRecordModifier.modifyRecord(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldProtectSpecificDataField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00156nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"690\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"690\":{\"subfields\":[{\"9\":\"local\"}],\"ind1\":\"9\",\"ind2\":\"9\"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00124nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"690\":{\"subfields\":[{\"9\":\"local\"}],\"ind1\":\"9\",\"ind2\":\"9\"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("690")
      .withIndicator1("9")
      .withIndicator2("9")
      .withSubfield("9")
      .withData("local");

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(Collections.singletonList(marcFieldProtectionSetting));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(mappingParameters));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("690")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("*"))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Collections.singletonList(mappingDetail)));

    //when
    marcRecordModifier.initialize(eventPayload, mappingParameters, mappingProfile);
    marcRecordModifier.modifyRecord(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldProtectFromAddingSubfieldToProtectedDataField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00151nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"690\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"690\":{\"subfields\":[{\"9\":\"local\"}],\"ind1\":\"9\",\"ind2\":\"9\"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00151nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"690\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"690\":{\"subfields\":[{\"9\":\"local\"}],\"ind1\":\"9\",\"ind2\":\"9\"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("690")
      .withIndicator1("*")
      .withIndicator2("*")
      .withSubfield("*")
      .withData("*");

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(Collections.singletonList(marcFieldProtectionSetting));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.ADD)
      .withField(new MarcField()
        .withField("690")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("c").withData(new Data().withText("new data")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Collections.singletonList(mappingDetail)));

    //when
    marcRecordModifier.initialize(eventPayload, mappingParameters, mappingProfile);
    marcRecordModifier.modifyRecord(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }


  @Test
  public void shouldDeleteUnprotectedDataField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00129nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}},{\"999\":{\"subfields\":[{\"s\":\"original\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00129nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("999")
      .withIndicator1("f")
      .withIndicator2("f")
      .withSubfield("*")
      .withData("*");

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(Collections.singletonList(marcFieldProtectionSetting));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("999")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("s"))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Collections.singletonList(mappingDetail)));

    //when
    marcRecordModifier.initialize(eventPayload, mappingParameters, mappingProfile);
    marcRecordModifier.modifyRecord(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldReturnEmptyListIfThereIsNoSettings() {
    List<MarcFieldProtectionSetting> marcFieldProtectionSettings = new ArrayList<>();
    List<MarcFieldProtectionSetting> protectionSettingsOverrides = Collections.singletonList(
      new MarcFieldProtectionSetting()
        .withId(UUID.randomUUID().toString())
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(true));

    assertTrue(marcRecordModifier.filterOutOverriddenProtectionSettings(marcFieldProtectionSettings, protectionSettingsOverrides).isEmpty());
  }

  @Test
  public void shouldReturnSameSettingsIfNoOverrides() {
    List<MarcFieldProtectionSetting> marcFieldProtectionSettings = Collections.singletonList(
      new MarcFieldProtectionSetting()
        .withId(UUID.randomUUID().toString())
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false));
    List<MarcFieldProtectionSetting> protectionSettingsOverrides = new ArrayList<>();

    assertEquals(marcFieldProtectionSettings, marcRecordModifier.filterOutOverriddenProtectionSettings(marcFieldProtectionSettings, protectionSettingsOverrides));
  }

  @Test
  public void shouldFilterOutOverriddenFieldProtectionSettings() {
    List<MarcFieldProtectionSetting> marcFieldProtectionSettings = Arrays.asList(
      new MarcFieldProtectionSetting()
        .withId("76669a02-a3d4-41af-9392-58502eaacd10")
        .withField("001")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.SYSTEM)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("480f0b23-0cbe-4a5c-b1f1-568b3216ff68")
        .withField("999")
        .withIndicator1("f")
        .withIndicator2("f")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.SYSTEM)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("2ef38de1-73aa-4e02-ae37-44e7148f414e")
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("c4bd5ddb-55de-467a-b824-c9e58822d006")
        .withField("650")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("6a13e600-a126-4d02-bc16-abd9ea7bed7c")
        .withField("700")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("bdd4b0cb-f598-4d6b-bbbe-3bcfc658e85f")
        .withField("035")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false));

    List<MarcFieldProtectionSetting> protectionSettingsOverrides = Arrays.asList(
      new MarcFieldProtectionSetting()
        .withId("2ef38de1-73aa-4e02-ae37-44e7148f414e")
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(true),
      new MarcFieldProtectionSetting()
        .withId("c4bd5ddb-55de-467a-b824-c9e58822d006")
        .withField("650")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(true),
      new MarcFieldProtectionSetting()
        .withId("480f0b23-0cbe-4a5c-b1f1-568b3216ff68")
        .withField("999")
        .withIndicator1("f")
        .withIndicator2("f")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.SYSTEM)
        .withOverride(true),
      new MarcFieldProtectionSetting()
        .withId("bdd4b0cb-f598-4d6b-bbbe-3bcfc658e85f")
        .withField("035")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("2557b110-df80-496d-aa04-d6549bc13a28")
        .withField("040")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(true)
    );

    List<MarcFieldProtectionSetting> expectedRelevantProtectionSettings = Arrays.asList(
      new MarcFieldProtectionSetting()
        .withId("76669a02-a3d4-41af-9392-58502eaacd10")
        .withField("001")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.SYSTEM)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("480f0b23-0cbe-4a5c-b1f1-568b3216ff68")
        .withField("999")
        .withIndicator1("f")
        .withIndicator2("f")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.SYSTEM)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("6a13e600-a126-4d02-bc16-abd9ea7bed7c")
        .withField("700")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("bdd4b0cb-f598-4d6b-bbbe-3bcfc658e85f")
        .withField("035")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false)
    );

    List<MarcFieldProtectionSetting> actual =
      marcRecordModifier.filterOutOverriddenProtectionSettings(marcFieldProtectionSettings, protectionSettingsOverrides);

    assertEquals(expectedRelevantProtectionSettings.size(), actual.size());
    expectedRelevantProtectionSettings.forEach(setting ->
      assertTrue(actual.stream().anyMatch(actualSetting -> setting.getId().equals(actualSetting.getId()))));
  }

}
