package org.folio.processing.mapping.writer.marc;

import io.vertx.core.json.Json;
import org.folio.DataImportEventPayload;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordWriter;
import org.folio.processing.value.MarcDetailValue;
import org.folio.rest.jaxrs.model.Data;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MarcSubfield.Position.AFTER_STRING;
import static org.folio.rest.jaxrs.model.MarcSubfield.Position.BEFORE_STRING;
import static org.folio.rest.jaxrs.model.MarcSubfield.Position.NEW_SUBFIELD;
import static org.folio.rest.jaxrs.model.MarcSubfield.Subaction.ADD_TO_EXISTING_FIELD;
import static org.folio.rest.jaxrs.model.MarcSubfield.Subaction.CREATE_NEW_FIELD;
import static org.folio.rest.jaxrs.model.MarcSubfield.Subaction.INSERT;
import static org.folio.rest.jaxrs.model.MarcSubfield.Subaction.REMOVE;
import static org.folio.rest.jaxrs.model.MarcSubfield.Subaction.REPLACE;

@RunWith(JUnit4.class)
public class MarcRecordWriterTest {

  private MarcRecordWriter marcRecordWriter = new MarcRecordWriter(MARC_BIBLIOGRAPHIC);

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenHasNoMarcRecord() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(new HashMap<>());
    marcRecordWriter.initialize(eventPayload);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.ADD)
      .withField(new MarcField()
        .withField("003")
        .withSubfields(Arrays.asList(new MarcSubfield()
          .withData(new Data().withText("OCoLC")))));
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    eventPayload.setContext(context);

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("007"));
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);

    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
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
    //when
    marcRecordWriter.initialize(eventPayload);
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

}
