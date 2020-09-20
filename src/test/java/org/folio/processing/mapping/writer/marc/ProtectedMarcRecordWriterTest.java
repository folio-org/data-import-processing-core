package org.folio.processing.mapping.writer.marc;

import io.vertx.core.json.Json;
import org.folio.DataImportEventPayload;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordWriter;
import org.folio.processing.value.MarcDetailValue;
import org.folio.rest.jaxrs.model.Data;
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
public class ProtectedMarcRecordWriterTest {

  private MarcRecordWriter marcRecordWriter = new MarcRecordWriter(MARC_BIBLIOGRAPHIC);

  @Test
  public void shouldProtectControlField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00097nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = parsedContent;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("001")
      .withData("*");

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("001")
        .withIndicator1(null)
        .withIndicator2(null)
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield(""))));
    //when
    marcRecordWriter.initializeWithProtectionSettings(eventPayload, Collections.singletonList(marcFieldProtectionSetting));
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldDeleteUnprotectControlField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00097nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"002\":\"whatever\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00097nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("001")
      .withData("*");

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("002")
        .withIndicator1(null)
        .withIndicator2(null)
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield(""))));
    //when
    marcRecordWriter.initializeWithProtectionSettings(eventPayload, Collections.singletonList(marcFieldProtectionSetting));
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldProtectDataField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00129nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    String expectedParsedContent = parsedContent;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("999")
      .withIndicator1("f")
      .withIndicator2("f")
      .withSubfield("*")
      .withData("*");

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
    //when
    marcRecordWriter.initializeWithProtectionSettings(eventPayload, Collections.singletonList(marcFieldProtectionSetting));
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldDeleteUnprotectDataField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00129nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}},{\"999\":{\"subfields\":[{\"a\":\"original\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00129nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("999")
      .withIndicator1("f")
      .withIndicator2("f")
      .withSubfield("*")
      .withData("*");

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("999")
        .withIndicator1(" ")
        .withIndicator2(" ")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("a"))));
    //when
    marcRecordWriter.initializeWithProtectionSettings(eventPayload, Collections.singletonList(marcFieldProtectionSetting));
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldProtectAnyDataFieldWithSpecifiedSubfieldAndData() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00156nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\"0\",\"ind2\":\"4\"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    String expectedParsedContent = parsedContent;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("*")
      .withIndicator1("*")
      .withIndicator2("*")
      .withSubfield("a")
      .withData("electronic");

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("*"))));
    //when
    marcRecordWriter.initializeWithProtectionSettings(eventPayload, Collections.singletonList(marcFieldProtectionSetting));
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldProtectDataFieldRegardlessItsIndicatorsAndSubfields() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00156nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"035\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\"0\",\"ind2\":\"4\"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    String expectedParsedContent = parsedContent;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("035")
      .withIndicator1("*")
      .withIndicator2("*")
      .withSubfield("*")
      .withData("*");

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("035")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("*"))));
    //when
    marcRecordWriter.initializeWithProtectionSettings(eventPayload, Collections.singletonList(marcFieldProtectionSetting));
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldProtectSpecificDataField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00156nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"690\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"690\":{\"subfields\":[{\"9\":\"local\"}],\"ind1\":\"9\",\"ind2\":\"9\"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    String expectedParsedContent = "{\"leader\":\"00124nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"690\":{\"subfields\":[{\"9\":\"local\"}],\"ind1\":\"9\",\"ind2\":\"9\"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("690")
      .withIndicator1("9")
      .withIndicator2("9")
      .withSubfield("9")
      .withData("local");

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("690")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("*"))));
    //when
    marcRecordWriter.initializeWithProtectionSettings(eventPayload, Collections.singletonList(marcFieldProtectionSetting));
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldDeleteUnprotectedDataField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00129nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}},{\"999\":{\"subfields\":[{\"s\":\"original\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00129nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"electronic\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"860d4528-3144-485a-bc63-841f22b12501\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("999")
      .withIndicator1("f")
      .withIndicator2("f")
      .withSubfield("*")
      .withData("*");

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("999")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("s"))));
    //when
    marcRecordWriter.initializeWithProtectionSettings(eventPayload, Collections.singletonList(marcFieldProtectionSetting));
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }

  @Test
  public void shouldNotMoveDataFromProtectedField() throws IOException {
    // given
    String parsedContent = "{\"leader\":\"00082nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"020\":{\"subfields\":[{\"a\":\"(electronic bk.)\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = parsedContent;

    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MarcFieldProtectionSetting marcFieldProtectionSetting = new MarcFieldProtectionSetting()
      .withField("020")
      .withIndicator1("*")
      .withIndicator2("*")
      .withSubfield("*")
      .withData("*");

    MarcField newFieldRule = new MarcField()
      .withField("991")
      .withIndicator1("1")
      .withIndicator2("1")
      .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("c")));

    MarcMappingDetail mappingDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.MOVE)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1(" ")
        .withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("a")
          .withSubaction(CREATE_NEW_FIELD)
          .withData(new Data().withMarcField(newFieldRule)))));
    //when
    marcRecordWriter.initializeWithProtectionSettings(eventPayload, Collections.singletonList(marcFieldProtectionSetting));
    marcRecordWriter.write(mappingDetail.getField().getField(), MarcDetailValue.of(mappingDetail));
    marcRecordWriter.getResult(eventPayload);
    //then
    String recordJson = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    Record actualRecord = Json.mapper.readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
  }


}
