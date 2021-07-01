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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import static io.vertx.core.json.jackson.DatabindCodec.mapper;
import static org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier.MATCHED_MARC_BIB_KEY;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MappingDetail.MarcMappingOption.MODIFY;
import static org.folio.rest.jaxrs.model.MappingDetail.MarcMappingOption.UPDATE;
import static org.folio.rest.jaxrs.model.MarcSubfield.Position.AFTER_STRING;
import static org.folio.rest.jaxrs.model.MarcSubfield.Position.BEFORE_STRING;
import static org.folio.rest.jaxrs.model.MarcSubfield.Position.NEW_SUBFIELD;
import static org.folio.rest.jaxrs.model.MarcSubfield.Subaction.ADD_TO_EXISTING_FIELD;
import static org.folio.rest.jaxrs.model.MarcSubfield.Subaction.CREATE_NEW_FIELD;
import static org.folio.rest.jaxrs.model.MarcSubfield.Subaction.INSERT;
import static org.folio.rest.jaxrs.model.MarcSubfield.Subaction.REMOVE;
import static org.folio.rest.jaxrs.model.MarcSubfield.Subaction.REPLACE;

@RunWith(JUnit4.class)
public class MarcRecordModifierTest {

  public static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private MarcRecordModifier marcRecordModifier = new MarcRecordModifier();

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenHasNoMarcRecord() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(new HashMap<>());
    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail().withMarcMappingOption(MODIFY));
    marcRecordModifier.initialize(eventPayload, mappingProfile);
  }

  @Test
  public void shouldAddSortableDataFieldInNumericalOrder() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00119nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"025\":{\"subfields\":[{\"a\":\"green\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(new MappingParameters()));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.ADD)
      .withField(new MarcField()
        .withField("025")
        .withIndicator1(null)
        .withIndicator2(null)
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("a")
          .withData(new Data().withText("green")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldAddNotSortableDataFieldAfterFieldsWithSameFirstDigit() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"538\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"650\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00096nam  22000611a 4500\",\"fields\":[{\"538\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"index\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"650\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(new MappingParameters()));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.ADD)
      .withField(new MarcField()
        .withField("500")
        .withIndicator1(" ")
        .withIndicator2(" ")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("a")
          .withData(new Data().withText("index")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldAddControlFieldInNumericalOrder() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"005\":\"20191122134835.0\"}]}";
    String expectedParsedContent = "{\"leader\":\"00096nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"003\":\"OCoLC\"},{\"005\":\"20191122134835.0\"}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(new MappingParameters()));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.ADD)
      .withField(new MarcField()
        .withField("003")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withData(new Data().withText("OCoLC")))));
    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));

    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldDeleteControlFieldByTag() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"007\":\"vz|cza\"}]}";
    String expectedParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(new MappingParameters()));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("007"));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldDeleteFieldsWithAnyIndicatorsByTag() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\": \"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\": \" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

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
        .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("*"))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldDeleteOnlyFieldsByTagAndSpecifiedIndicators() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00076nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

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
        .withIndicator2("0")
        .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("*"))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldDeleteOnlySpecifiedSubfieldFromFieldByTag() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\":\"green\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"a\":\"book\"},{\"b\":\"red\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00068nam  22000491a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"b\":\"green\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"b\":\"red\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

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
        .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("a"))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldDeleteEntireFieldWhenItContainsOnlySpecifiedSubfield() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\":\"green\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"a\":\"book\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00048nam  22000371a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"b\":\"green\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1(null)
        .withIndicator2(null)
        .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("a"))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldMoveDataToSpecifiedSubfieldOfNewFieldAndDeleteSourceField() throws IOException {
    // given
    String parsedContent = "{\"leader\": \"01314nam  22003851a 4500\", \"fields\": [{\"001\": \"ybp7406411\"}, {\"020\": {\"subfields\": [{\"a\": \"(electronic bk.)\"}], \"ind1\": \" \", \"ind2\": \" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00082nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"991\":{\"subfields\":[{\"c\":\"(electronic bk.)\"}],\"ind1\":\"1\",\"ind2\":\"1\"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcField newFieldRule = new MarcField()
      .withField("991")
      .withIndicator1("1")
      .withIndicator2("1")
      .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("c")));

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.MOVE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1(" ")
        .withIndicator2("*")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("a")
          .withSubaction(CREATE_NEW_FIELD)
          .withData(new Data().withMarcField(newFieldRule)))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldMoveAllSubfieldsDataToNewFieldWithSourceIndicatorsAndDeleteSourceFieldWhenWildcardSubfield() throws IOException {
    // given
    String parsedContent = "{\"leader\": \"01314nam  22003851a 4500\", \"fields\": [{\"020\": {\"subfields\": [{\"a\": \"electronic bk\"}, {\"b\": \"256\"}, {\"c\": \"128\"}], \"ind1\": \"7\",\"ind2\": \" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00066nam  22000371a 4500\",\"fields\":[{\"991\":{\"subfields\":[{\"a\":\"electronic bk\"},{\"b\":\"256\"},{\"c\":\"128\"}],\"ind1\":\"7\",\"ind2\":\" \"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcField newFieldRule = new MarcField()
      .withField("991")
      .withIndicator1(null)
      .withIndicator2(null);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.MOVE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1("7")
        .withIndicator2("*")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("*")
          .withSubaction(CREATE_NEW_FIELD)
          .withData(new Data().withMarcField(newFieldRule)))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldMoveDataToSpecifiedSubfieldOfNewFieldAndDeleteOnlyMovedSubfield() throws IOException {
    // given
    String parsedContent = "{\"leader\": \"01314nam  22003851a 4500\", \"fields\":[{\"020\":{\"subfields\":[{\"a\":\"(electronic bk.)\"},{\"b\": \"green\"}],\"ind1\": \"7\", \"ind2\": \"7\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00081nam  22000491a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"b\":\"green\"}],\"ind1\":\"7\",\"ind2\":\"7\"}},{\"991\":{\"subfields\":[{\"z\":\"(electronic bk.)\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcField newFieldRule = new MarcField()
      .withField("991")
      .withIndicator1(" ")
      .withIndicator2(" ")
      .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("z")));

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.MOVE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("7")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("a")
          .withSubaction(CREATE_NEW_FIELD)
          .withData(new Data().withMarcField(newFieldRule)))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldNotMoveDataToNewFieldWhenSourceFieldWithSpecifiedSubfieldDoesNotExist() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00059nam  22000371a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"a\":\"(electronic bk.)\"}],\"ind1\":\"7\",\"ind2\":\"7\"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcField existingFieldsRule = new MarcField()
      .withField("993")
      .withIndicator1(" ")
      .withIndicator2(" ")
      .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("w")));

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.MOVE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("7")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("b")
          .withSubaction(CREATE_NEW_FIELD)
          .withData(new Data().withMarcField(existingFieldsRule)))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(parsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldMoveDataToNewSortableFieldInNumericalOrder() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\": \"green\"}],\"ind1\": \"7\",\"ind2\": \"7\"}},{\"022\":{\"subfields\":[{\"a\":\"red\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00095nam  22000611a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"b\":\"green\"}],\"ind1\":\"7\",\"ind2\":\"7\"}},{\"021\":{\"subfields\":[{\"z\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"022\":{\"subfields\":[{\"a\":\"red\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcField newFieldRule = new MarcField()
      .withField("021")
      .withIndicator1(" ")
      .withIndicator2(" ")
      .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("z")));

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.MOVE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("7")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("a")
          .withSubaction(CREATE_NEW_FIELD)
          .withData(new Data().withMarcField(newFieldRule)))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldMoveDataToExistingFieldsAndDeleteSourceField() throws IOException {
    // given
    String parsedContent = "{\"leader\": \"01314nam  22003851a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"993\":{\"subfields\":[{\"x\":\"one\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"993\":{\"subfields\":[{\"y\": \"two\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00090nam  22000491a 4500\",\"fields\":[{\"993\":{\"subfields\":[{\"x\":\"one\"},{\"w\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"993\":{\"subfields\":[{\"y\":\"two\"},{\"w\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcField existingFieldsRule = new MarcField()
      .withField("993")
      .withIndicator1(" ")
      .withIndicator2(" ")
      .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("w")));

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.MOVE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1("*")
        .withIndicator2(" ")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("a")
          .withSubaction(ADD_TO_EXISTING_FIELD)
          .withData(new Data().withMarcField(existingFieldsRule)))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldMoveDataToExistingFieldsAndDeleteOnlyMovedSubfield() throws IOException {
    // given
    String parsedContent = "{\"leader\": \"01314nam  22003851a 4500\", \"fields\":[{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\": \"green\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"993\":{\"subfields\":[{\"x\":\"one\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"993\":{\"subfields\":[{\"y\": \"two\"}],\"ind1\": \" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00112nam  22000611a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"b\":\"green\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"993\":{\"subfields\":[{\"x\":\"one\"},{\"w\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"993\":{\"subfields\":[{\"y\":\"two\"},{\"w\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcField existingFieldsRule = new MarcField()
      .withField("993")
      .withIndicator1(" ")
      .withIndicator2(" ")
      .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("w")));

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.MOVE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1("*")
        .withIndicator2(" ")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("a")
          .withSubaction(ADD_TO_EXISTING_FIELD)
          .withData(new Data().withMarcField(existingFieldsRule)))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldMoveDataFromMultipleSrcFieldsToExistingFieldsAndDeleteOnlyMovedSubfields() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\": \"green\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"a\":\"book\"},{\"f\": \"red\"}],\"ind1\":\" \",\"ind2\":\" \"}}, {\"993\":{\"subfields\":[{\"x\":\"one\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"993\":{\"subfields\":[{\"y\": \"two\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00144nam  22000731a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"b\":\"green\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"f\":\"red\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"993\":{\"subfields\":[{\"x\":\"one\"},{\"w\":\"electronic\"},{\"w\":\"book\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"993\":{\"subfields\":[{\"y\":\"two\"},{\"w\":\"electronic\"},{\"w\":\"book\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcField existingFieldsRule = new MarcField()
      .withField("993")
      .withIndicator1(" ")
      .withIndicator2(" ")
      .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("w")));

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.MOVE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1("*")
        .withIndicator2(" ")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("a")
          .withSubaction(ADD_TO_EXISTING_FIELD)
          .withData(new Data().withMarcField(existingFieldsRule)))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));

    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldNotMoveDataWhenSpecifiedDestinationFieldDoesNotExist() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00074nam  22000491a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"993\":{\"subfields\":[{\"a\":\"book\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcField existingFieldsRule = new MarcField()
      .withField("993")
      .withIndicator1("0")
      .withIndicator2("0")
      .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("w")));

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.MOVE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1("*")
        .withIndicator2(" ")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("a")
          .withSubaction(ADD_TO_EXISTING_FIELD)
          .withData(new Data().withMarcField(existingFieldsRule)))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(parsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldNotMoveDataToExistingFieldWhenSourceFieldWithSpecifiedSubfieldDoesNotExist() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00074nam  22000491a 4500\",\"fields\":[{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"993\":{\"subfields\":[{\"a\":\"book\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcField existingFieldsRule = new MarcField()
      .withField("993")
      .withIndicator1(" ")
      .withIndicator2(" ")
      .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("w")));

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.MOVE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1("*")
        .withIndicator2(" ")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("b")
          .withSubaction(ADD_TO_EXISTING_FIELD)
          .withData(new Data().withMarcField(existingFieldsRule)))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(parsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldInsertDataBeforeExistingToFieldWithSpecifiedIndicatorsAndSubfield() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"u\":\"example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00084nam  22000371a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("856")
        .withIndicator1(null)
        .withIndicator2(null)
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("u")
          .withSubaction(INSERT)
          .withPosition(BEFORE_STRING)
          .withData(new Data().withText("http://libproxy.smith.edu?url=")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldInsertDataAfterExistingToSpecifiedSubfieldOfMultipleFieldsWithAnyIndicators() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"905\":{\"subfields\":[{\"a\":\"music\"}],\"ind1\":\"a\",\"ind2\":\"a\"}},{\"905\":{\"subfields\":[{\"a\":\"art\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00114nam  22000491a 4500\",\"fields\":[{\"905\":{\"subfields\":[{\"a\":\"music; updated 28 April 2020\"}],\"ind1\":\"a\",\"ind2\":\"a\"}},{\"905\":{\"subfields\":[{\"a\":\"art; updated 28 April 2020\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("905")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("a")
          .withSubaction(INSERT)
          .withPosition(AFTER_STRING)
          .withData(new Data().withText("; updated 28 April 2020")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldInsertDataToNewSubfieldOfFieldWithSpecifiedIndicators() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"z\":\"electronic\"}],\"ind1\":\"4\",\"ind2\":\"1\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00080nam  22000371a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"z\":\"electronic\"},{\"z\":\"to access, click the link\"}],\"ind1\":\"4\",\"ind2\":\"1\"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("856")
        .withIndicator1("4")
        .withIndicator2("1")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("z")
          .withSubaction(INSERT)
          .withPosition(NEW_SUBFIELD)
          .withData(new Data().withText("to access, click the link")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldInsertDataBeforeExistingToFieldRegardlessIndicatorsAndSubfield() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"u\":\"example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00084nam  22000371a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("856")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("*")
          .withSubaction(INSERT)
          .withPosition(BEFORE_STRING)
          .withData(new Data().withText("http://libproxy.smith.edu?url=")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldReplaceDataIntoFieldWithAnyIndicatorsAndSubfields() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00068nam  22000371a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"a\":\"http://libproxy.smith.edu\"}],\"ind1\":\"4\",\"ind2\":\"1\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00069nam  22000371a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"a\":\"https://libproxy.smith.edu\"}],\"ind1\":\"4\",\"ind2\":\"1\"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("856")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("*")
          .withSubaction(REPLACE)
          .withData(new Data()
            .withFind("http://")
            .withReplaceWith("https://")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldReplaceAllDataIntoSubfieldOfFieldByWildcardData() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00068nam  22000371a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"c\":\"Church\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00051nam  22000371a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"c\":\"McCarthy\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("856")
        .withIndicator1(null)
        .withIndicator2(null)
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("c")
          .withSubaction(REPLACE)
          .withData(new Data()
            .withFind("*")
            .withReplaceWith("McCarthy")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldReplaceDataInControlFieldBySinglePosition() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"008\":\"121119s2013    vtu     ob    001 0 eng d\"}]}";
    String expectedParsedContent = "{\"leader\":\"00079nam  22000371a 4500\",\"fields\":[{\"008\":\"121119p2013    vtu     ob    001 0 eng d\"}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("008/06")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubaction(REPLACE)
          .withData(new Data()
            .withFind("s")
            .withReplaceWith("p")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldReplaceDataInControlFieldByPositionsRange() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"008\":\"121119s2019    vtu     ob    001 0 eng d\"}]}";
    String expectedParsedContent = "{\"leader\":\"00079nam  22000371a 4500\",\"fields\":[{\"008\":\"121119s2020    vtu     ob    001 0 eng d\"}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("008/07-10")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubaction(REPLACE)
          .withData(new Data()
            .withFind("2019")
            .withReplaceWith("2020")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldReplaceDataInLeaderFieldBySinglePosition() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"008\":\"121119s2013    vtu     ob    001 0 eng d\"}]}";
    String expectedParsedContent = "{\"leader\":\"00079cam  22000371a 4500\",\"fields\":[{\"008\":\"121119s2013    vtu     ob    001 0 eng d\"}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("LDR/05")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubaction(REPLACE)
          .withData(new Data()
            .withFind("n")
            .withReplaceWith("c")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldNotReplaceDataInLeaderFieldWhenSpecifiedNotMappablePositions() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00079nam  22000371a 4500\",\"fields\":[{\"008\":\"121119s2013    vtu     ob    001 0 eng d\"}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("LDR/01-04")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubaction(REPLACE)
          .withData(new Data()
            .withFind("00079")
            .withReplaceWith("00123")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(parsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldRemoveSpecifiedTextInFieldSubfield() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00068nam  22000371a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"a\":\"via CatWeb\"}],\"ind1\":\"4\",\"ind2\":\"1\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00050nam  22000371a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"a\":\"via Web\"}],\"ind1\":\"4\",\"ind2\":\"1\"}}]}";

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("856")
        .withIndicator1("4")
        .withIndicator2("*")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("*")
          .withSubaction(REMOVE)
          .withData(new Data()
            .withText("Cat")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);

    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldRemoveSpecifiedTextInControlFieldByPositionsRange() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"008\":\"121119s2019    vtu     ob    001 0 eng d\"}]}";
    String expectedParsedContent = "{\"leader\":\"00075nam  22000371a 4500\",\"fields\":[{\"008\":\"121119s    vtu     ob    001 0 eng d\"}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField()
        .withField("008/07-10")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubaction(REMOVE)
          .withData(new Data()
            .withText("2019")))));

    MappingProfile mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail()
      .withMarcMappingOption(MODIFY)
      .withMarcMappingDetails(Arrays.asList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.modifyRecord(Arrays.asList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldReplaceSpecifiedControlField() throws IOException {
    // given
    String incomingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"005\":\"5121024\"}]}";
    String existingParsedContent = "{\"leader\": \"01314nam  22003851a 4500\", \"fields\": [{\"001\": \"ybp7406411\"}, {\"005\":\"123123\"},{\"856\":{\"subfields\":[{\"u\":\"example.com\"}],\"ind1\":\"4\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00097nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"005\":\"5121024\"},{\"856\":{\"subfields\":[{\"u\":\"example.com\"}],\"ind1\":\"4\",\"ind2\":\"0\"}}]}";

    Record incomingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(incomingParsedContent));
    Record existingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(existingParsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(incomingRecord));
    context.put(MATCHED_MARC_BIB_KEY, Json.encodePrettily(existingRecord));
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(new MappingParameters()));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withField(new MarcField().withField("005"));

    MappingProfile mappingProfile = new MappingProfile()
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(UPDATE)
        .withMarcMappingDetails(Collections.singletonList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.processUpdateMappingOption(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MATCHED_MARC_BIB_KEY);
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldReplaceOnlySpecifiedSubfield() throws IOException {
    // given
    String incomingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\"4\",\"ind2\":\" \"}}]}";
    String existingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\", \"fields\": [{\"001\": \"ybp7406411\"}, {\"256\": {\"subfields\": [{\"a\": \"(electronic bk.)\"}], \"ind1\": \" \", \"ind2\": \" \"}}, {\"856\": {\"subfields\": [{\"u\": \"example.com\"},{\"z\":\"to access, click the link\"}], \"ind1\": \"4\", \"ind2\": \" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00167nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"256\":{\"subfields\":[{\"a\":\"(electronic bk.)\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"},{\"z\":\"to access, click the link\"}],\"ind1\":\"4\",\"ind2\":\" \"}}]}";

    Record incomingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(incomingParsedContent));
    Record existingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(existingParsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(incomingRecord));
    context.put(MATCHED_MARC_BIB_KEY, Json.encodePrettily(existingRecord));
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(new MappingParameters()));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withField(new MarcField()
        .withField("856")
        .withIndicator1("4")
        .withIndicator2(null)
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("u"))));

    MappingProfile mappingProfile = new MappingProfile()
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(UPDATE)
        .withMarcMappingDetails(Collections.singletonList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.processUpdateMappingOption(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MATCHED_MARC_BIB_KEY);
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldReplaceEntireExistingFieldWhenWildcardSubfield() throws IOException {
    // given
    String incomingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\"4\",\"ind2\":\"0\"}}]}";
    String existingParsedContent = "{\"leader\": \"01314nam  22003851a 4500\", \"fields\": [{\"001\": \"ybp7406411\"}, {\"256\": {\"subfields\": [{\"a\": \"(electronic bk.)\"}], \"ind1\": \" \", \"ind2\": \" \"}},{\"856\":{\"subfields\":[{\"u\":\"example.com\"},{\"z\":\"to access, click the link\"}],\"ind1\":\"4\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00140nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"256\":{\"subfields\":[{\"a\":\"(electronic bk.)\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\"4\",\"ind2\":\"0\"}}]}";

    Record incomingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(incomingParsedContent));
    Record existingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(existingParsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(incomingRecord));
    context.put(MATCHED_MARC_BIB_KEY, Json.encodePrettily(existingRecord));
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(new MappingParameters()));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withField(new MarcField()
        .withField("856")
        .withIndicator1("*")
        .withIndicator2("0")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("*"))));

    MappingProfile mappingProfile = new MappingProfile()
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(UPDATE)
        .withMarcMappingDetails(Collections.singletonList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.processUpdateMappingOption(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MATCHED_MARC_BIB_KEY);
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldRemoveAndAddNewValuesToAllNotProtectedFields() throws IOException {
    // given
    String expectedParsedContent = "{\"leader\":\"01016cam a2200169Ii 4500\",\"fields\":[{\"001\":\"on1032262463\"},{\"008\":\"180424s1914    enkaf         000 0 eng d\"},{\"400\":{\"subfields\":[{\"a\":\"Testing value for 400\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"400\":{\"subfields\":[{\"a\":\"Testing value for 400 - 2\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"400\":{\"subfields\":[{\"a\":\"Testing value for 400 - 3\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"Also published by Charles Scribner's Sons.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"505\":{\"subfields\":[{\"a\":\"Testing 505 field\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"Later printings substitute The foot in place of Decivilized.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"\\\"Of this edition on large handmade paper two hundred and fifty copies were printed, in May 1914, of which this is no. ...\\\"--Title page verso\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"\\\"Most of these essays are collected and selected from the volumes entitled The rhythm of life, The colour of life, The spirit of place, The children, and Ceres' runaway. In addition are \\\"The seventeenth century,\\\" \\\"Prue,\\\" \\\"Mrs. Johnson,\\\" and \\\"Madame Roland,\\\" here for the first time put into a book.\\\"--Title page verso\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"600\":{\"subfields\":[{\"a\":\"Testing value for 600\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"083837e5-009f-42b3-940a-beef28dc90e9\"},{\"i\":\"59efa5f1-1b1d-456c-bd65-c6783c9c5fc4\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    String existingParsedContent = "{\"id\":\"f3ff7ef8-18b5-48e2-9e4a-5f78ba0c8164\",\"snapshotId\":\"b27b2c06-c109-4c57-9bf4-ecca01bad37d\",\"matchedId\":\"f3ff7ef8-18b5-48e2-9e4a-5f78ba0c8164\",\"generation\":0,\"recordType\":\"MARC\",\"rawRecord\":{\"id\":\"f3ff7ef8-18b5-48e2-9e4a-5f78ba0c8164\"},\"parsedRecord\":{\"id\":\"f3ff7ef8-18b5-48e2-9e4a-5f78ba0c8164\",\"content\":{\"fields\":[{\"001\":\"in00000000009\"},{\"008\":\"180424s1914    enkaf         000 0 eng d\"},{\"330\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 330\"}]}},{\"400\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 400\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Also published by Charles Scribner's Sons.\"}]}},{\"505\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing 505 field\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Later printings substitute The foot in place of Decivilized.\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"\\\"Of this edition on large handmade paper two hundred and fifty copies were printed, in May 1914, of which this is no. ...\\\"--Title page verso\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"\\\"Most of these essays are collected and selected from the volumes entitled The rhythm of life, The colour of life, The spirit of place, The children, and Ceres' runaway. In addition are \\\"The seventeenth century,\\\" \\\"Prue,\\\" \\\"Mrs. Johnson,\\\" and \\\"Madame Roland,\\\" here for the first time put into a book.\\\"--Title page verso\"}]}},{\"600\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 600\"}]}},{\"600\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 600 - 2\"}]}},{\"600\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 600 - 3\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"f3ff7ef8-18b5-48e2-9e4a-5f78ba0c8164\"},{\"i\":\"baa69d84-b3ee-49a7-8946-8f4257cb698a\"}]}}],\"leader\":\"03447cam a2200481Ii 4500\"}},\"deleted\":false,\"order\":0,\"externalIdsHolder\":{\"instanceId\":\"baa69d84-b3ee-49a7-8946-8f4257cb698a\",\"instanceHrid\":\"in00000000009\"},\"additionalInfo\":{\"suppressDiscovery\":false},\"state\":\"ACTUAL\",\"leaderRecordStatus\":\"c\",\"metadata\":{\"createdDate\":1619775341915,\"updatedDate\":1619775342710}}";
    String incomingParsedContent = "{\"id\":\"083837e5-009f-42b3-940a-beef28dc90e9\",\"snapshotId\":\"1f563c7a-7b57-4320-9152-f3769f6dedc7\",\"matchedId\":\"083837e5-009f-42b3-940a-beef28dc90e9\",\"generation\":0,\"recordType\":\"MARC\",\"rawRecord\":{\"id\":\"083837e5-009f-42b3-940a-beef28dc90e9\"},\"parsedRecord\":{\"id\":\"083837e5-009f-42b3-940a-beef28dc90e9\",\"content\":{\"fields\":[{\"001\":\"on1032262463\"},{\"400\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 400\"}]}},{\"400\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 400 - 2\"}]}},{\"400\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 400 - 3\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Also published by Charles Scribner's Sons.\"}]}},{\"505\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing 505 field\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Later printings substitute The foot in place of Decivilized.\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"\\\"Of this edition on large handmade paper two hundred and fifty copies were printed, in May 1914, of which this is no. ...\\\"--Title page verso\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"\\\"Most of these essays are collected and selected from the volumes entitled The rhythm of life, The colour of life, The spirit of place, The children, and Ceres' runaway. In addition are \\\"The seventeenth century,\\\" \\\"Prue,\\\" \\\"Mrs. Johnson,\\\" and \\\"Madame Roland,\\\" here for the first time put into a book.\\\"--Title page verso\"}]}},{\"600\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 600\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"083837e5-009f-42b3-940a-beef28dc90e9\"},{\"i\":\"59efa5f1-1b1d-456c-bd65-c6783c9c5fc4\"}]}}],\"leader\":\"03464cam a2200493Ii 4500\"}},\"deleted\":false,\"order\":0,\"state\":\"ACTUAL\",\"metadata\":{\"createdDate\":1619775341915,\"updatedDate\":1619775342710}}";

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), incomingParsedContent);
    context.put(MATCHED_MARC_BIB_KEY,existingParsedContent);
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(new MappingParameters()));
    eventPayload.setContext(context);

    MappingProfile mappingProfile = new MappingProfile()
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(UPDATE)
        .withRecordType(MARC_BIBLIOGRAPHIC));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.processUpdateMappingOption(Collections.emptyList());
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MATCHED_MARC_BIB_KEY);
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldAddIncomingFieldToExistingRecordWhenNoCorrespondingExistingField() throws IOException {
    // given
    String incomingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"590\":{\"subfields\":[{\"a\":\"excelsior\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String existingParsedContent = "{\"leader\": \"01314nam  22003851a 4500\", \"fields\": [{\"001\": \"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"example.com\"},{\"z\":\"to access, click the link\"}],\"ind1\":\"4\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00130nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"590\":{\"subfields\":[{\"a\":\"excelsior\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"example.com\"},{\"z\":\"to access, click the link\"}],\"ind1\":\"4\",\"ind2\":\"0\"}}]}";

    Record incomingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(incomingParsedContent));
    Record existingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(existingParsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(incomingRecord));
    context.put(MATCHED_MARC_BIB_KEY, Json.encodePrettily(existingRecord));
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(new MappingParameters()));
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withField(new MarcField()
        .withField("590")
        .withIndicator1(" ")
        .withIndicator2(" ")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withSubfield("*"))));

    MappingProfile mappingProfile = new MappingProfile()
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(UPDATE)
        .withMarcMappingDetails(Collections.singletonList(mappingDetail)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.processUpdateMappingOption(Collections.singletonList(mappingDetail));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MATCHED_MARC_BIB_KEY);
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldReplaceExistingFieldsWithAllIncomingFieldsWhenNoMarcMappingDetails() throws IOException {
    // given
    String incomingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"650\":{\"subfields\":[{\"a\":\"video\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"Ritchie\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String existingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp1234567\"},{\"650\":{\"subfields\":[{\"a\":\"motion\"},{\"b\":\"pictures\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"Kernighan\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"example.org\"}],\"ind1\":\"4\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00095nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"650\":{\"subfields\":[{\"a\":\"video\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"Ritchie\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record incomingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(incomingParsedContent));
    Record existingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(existingParsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(incomingRecord));
    context.put(MATCHED_MARC_BIB_KEY, Json.encodePrettily(existingRecord));
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(new MappingParameters()));
    eventPayload.setContext(context);

    MappingProfile mappingProfile = new MappingProfile()
      .withMappingDetails(new MappingDetail().withMarcMappingOption(UPDATE));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.processUpdateMappingOption(Collections.emptyList());
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MATCHED_MARC_BIB_KEY);
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldRemoveAndAddNewValuesToAllNotProtectedFieldsAndUpdateLeader() throws IOException {
    // given
    String expectedParsedContent = "{\"leader\":\"01016cas a2200169Ii 4500\",\"fields\":[{\"001\":\"on1032262463\"},{\"008\":\"180424s1914    enkaf         000 0 eng d\"},{\"400\":{\"subfields\":[{\"a\":\"Testing value for 400\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"400\":{\"subfields\":[{\"a\":\"Testing value for 400 - 2\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"400\":{\"subfields\":[{\"a\":\"Testing value for 400 - 3\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"Also published by Charles Scribner's Sons.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"505\":{\"subfields\":[{\"a\":\"Testing 505 field\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"Later printings substitute The foot in place of Decivilized.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"\\\"Of this edition on large handmade paper two hundred and fifty copies were printed, in May 1914, of which this is no. ...\\\"--Title page verso\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"\\\"Most of these essays are collected and selected from the volumes entitled The rhythm of life, The colour of life, The spirit of place, The children, and Ceres' runaway. In addition are \\\"The seventeenth century,\\\" \\\"Prue,\\\" \\\"Mrs. Johnson,\\\" and \\\"Madame Roland,\\\" here for the first time put into a book.\\\"--Title page verso\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"600\":{\"subfields\":[{\"a\":\"Testing value for 600\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"083837e5-009f-42b3-940a-beef28dc90e9\"},{\"i\":\"59efa5f1-1b1d-456c-bd65-c6783c9c5fc4\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    String existingParsedContent = "{\"id\":\"f3ff7ef8-18b5-48e2-9e4a-5f78ba0c8164\",\"snapshotId\":\"b27b2c06-c109-4c57-9bf4-ecca01bad37d\",\"matchedId\":\"f3ff7ef8-18b5-48e2-9e4a-5f78ba0c8164\",\"generation\":0,\"rawRecord\":{\"id\":\"f3ff7ef8-18b5-48e2-9e4a-5f78ba0c8164\"},\"parsedRecord\":{\"id\":\"f3ff7ef8-18b5-48e2-9e4a-5f78ba0c8164\",\"content\":{\"fields\":[{\"001\":\"in00000000009\"},{\"008\":\"180424s1914    enkaf         000 0 eng d\"},{\"330\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 330\"}]}},{\"400\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 400\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Also published by Charles Scribner's Sons.\"}]}},{\"505\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing 505 field\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Later printings substitute The foot in place of Decivilized.\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"\\\"Of this edition on large handmade paper two hundred and fifty copies were printed, in May 1914, of which this is no. ...\\\"--Title page verso\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"\\\"Most of these essays are collected and selected from the volumes entitled The rhythm of life, The colour of life, The spirit of place, The children, and Ceres' runaway. In addition are \\\"The seventeenth century,\\\" \\\"Prue,\\\" \\\"Mrs. Johnson,\\\" and \\\"Madame Roland,\\\" here for the first time put into a book.\\\"--Title page verso\"}]}},{\"600\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 600\"}]}},{\"600\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 600 - 2\"}]}},{\"600\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 600 - 3\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"f3ff7ef8-18b5-48e2-9e4a-5f78ba0c8164\"},{\"i\":\"baa69d84-b3ee-49a7-8946-8f4257cb698a\"}]}}],\"leader\":\"03447cam a2200481Ii 4500\"}},\"deleted\":false,\"order\":0,\"externalIdsHolder\":{\"instanceId\":\"baa69d84-b3ee-49a7-8946-8f4257cb698a\",\"instanceHrid\":\"in00000000009\"},\"additionalInfo\":{\"suppressDiscovery\":false},\"state\":\"ACTUAL\",\"leaderRecordStatus\":\"c\",\"metadata\":{\"createdDate\":1619775341915,\"updatedDate\":1619775342710}}";
    String incomingParsedContent = "{\"id\":\"083837e5-009f-42b3-940a-beef28dc90e9\",\"snapshotId\":\"1f563c7a-7b57-4320-9152-f3769f6dedc7\",\"matchedId\":\"083837e5-009f-42b3-940a-beef28dc90e9\",\"generation\":0,\"rawRecord\":{\"id\":\"083837e5-009f-42b3-940a-beef28dc90e9\"},\"parsedRecord\":{\"id\":\"083837e5-009f-42b3-940a-beef28dc90e9\",\"content\":{\"fields\":[{\"001\":\"on1032262463\"},{\"400\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 400\"}]}},{\"400\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 400 - 2\"}]}},{\"400\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 400 - 3\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Also published by Charles Scribner's Sons.\"}]}},{\"505\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing 505 field\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Later printings substitute The foot in place of Decivilized.\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"\\\"Of this edition on large handmade paper two hundred and fifty copies were printed, in May 1914, of which this is no. ...\\\"--Title page verso\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"\\\"Most of these essays are collected and selected from the volumes entitled The rhythm of life, The colour of life, The spirit of place, The children, and Ceres' runaway. In addition are \\\"The seventeenth century,\\\" \\\"Prue,\\\" \\\"Mrs. Johnson,\\\" and \\\"Madame Roland,\\\" here for the first time put into a book.\\\"--Title page verso\"}]}},{\"600\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Testing value for 600\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"083837e5-009f-42b3-940a-beef28dc90e9\"},{\"i\":\"59efa5f1-1b1d-456c-bd65-c6783c9c5fc4\"}]}}],\"leader\":\"03464cas a2200493Ii 4500\"}},\"deleted\":false,\"order\":0,\"state\":\"ACTUAL\",\"metadata\":{\"createdDate\":1619775341915,\"updatedDate\":1619775342710}}";

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), incomingParsedContent);
    context.put(MATCHED_MARC_BIB_KEY,existingParsedContent);
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(new MappingParameters()));
    eventPayload.setContext(context);

    MappingProfile mappingProfile = new MappingProfile()
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(UPDATE)
        .withRecordType(MARC_BIBLIOGRAPHIC));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.processUpdateMappingOption(Collections.emptyList());
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MATCHED_MARC_BIB_KEY);
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldNotReplaceProtectedExistingField() throws IOException {
    // given
    String incomingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"650\":{\"subfields\":[{\"a\":\"video\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"Ritchie\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String existingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"650\":{\"subfields\":[{\"a\":\"pictures\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"Kernighan\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00098nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"650\":{\"subfields\":[{\"a\":\"pictures\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"Ritchie\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record incomingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(incomingParsedContent));
    Record existingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(existingParsedContent));

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(Arrays.asList(new MarcFieldProtectionSetting()
        .withField("650")
        .withSubfield("a")
        .withIndicator1(" ")
        .withIndicator2("*")
        .withData("pictures")));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(incomingRecord));
    context.put(MATCHED_MARC_BIB_KEY, Json.encodePrettily(existingRecord));
    context.put("MAPPING_PARAMS", Json.encodePrettily(mappingParameters));
    eventPayload.setContext(context);

    MarcMappingDetail mappingRule1 = new MarcMappingDetail()
      .withOrder(0)
      .withField(new MarcField()
        .withField("650")
        .withIndicator1(" ")
        .withIndicator2("*")
        .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("a"))));

    MarcMappingDetail mappingRule2 = new MarcMappingDetail()
      .withOrder(0)
      .withField(new MarcField()
        .withField("700")
        .withIndicator1(null)
        .withIndicator2(null)
        .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("a"))));

    MappingProfile mappingProfile = new MappingProfile()
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(UPDATE)
        .withMarcMappingDetails(Arrays.asList(mappingRule1, mappingRule2)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.processUpdateMappingOption(Arrays.asList(mappingRule1, mappingRule2));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MATCHED_MARC_BIB_KEY);
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldReplaceOverriddenProtectedExistingField() throws IOException {
    // given
    String incomingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"650\":{\"subfields\":[{\"a\":\"video\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"Ritchie\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String existingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"650\":{\"subfields\":[{\"a\":\"video\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"Kernighan\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00095nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"650\":{\"subfields\":[{\"a\":\"video\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"Ritchie\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    Record incomingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(incomingParsedContent));
    Record existingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(existingParsedContent));

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withId(UUID.randomUUID().toString())
      .withField("650")
      .withSubfield("a")
      .withIndicator1(" ")
      .withIndicator2(" ")
      .withData("pictures")
      .withSource(MarcFieldProtectionSetting.Source.USER)
      .withOverride(true);

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(Arrays.asList(marcFieldProtectionSetting));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(incomingRecord));
    context.put(MATCHED_MARC_BIB_KEY, Json.encodePrettily(existingRecord));
    context.put(MAPPING_PARAMS_KEY, Json.encodePrettily(mappingParameters));
    eventPayload.setContext(context);

    MarcMappingDetail mappingRule1 = new MarcMappingDetail()
      .withOrder(0)
      .withField(new MarcField()
        .withField("650")
        .withIndicator1(" ")
        .withIndicator2("*")
        .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("a"))));

    MarcMappingDetail mappingRule2 = new MarcMappingDetail()
      .withOrder(0)
      .withField(new MarcField()
        .withField("700")
        .withIndicator1(null)
        .withIndicator2(null)
        .withSubfields(Arrays.asList(new MarcSubfield().withSubfield("a"))));

    MappingProfile mappingProfile = new MappingProfile()
      .withMarcFieldProtectionSettings(Arrays.asList(marcFieldProtectionSetting))
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(UPDATE)
        .withMarcMappingDetails(Arrays.asList(mappingRule1, mappingRule2)));
    //when
    marcRecordModifier.initialize(eventPayload, mappingProfile);
    marcRecordModifier.processUpdateMappingOption(Arrays.asList(mappingRule1, mappingRule2));
    marcRecordModifier.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MATCHED_MARC_BIB_KEY);
    Record actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

}
