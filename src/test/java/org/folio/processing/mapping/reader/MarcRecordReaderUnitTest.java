package org.folio.processing.mapping.reader;

import com.google.common.collect.Lists;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.value.BooleanValue;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.RepeatableFieldValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.processing.value.Value.ValueType;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.RepeatableSubfieldMapping;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MappingRule.RepeatableFieldAction.DELETE_EXISTING;
import static org.folio.rest.jaxrs.model.MappingRule.RepeatableFieldAction.EXTEND_EXISTING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class MarcRecordReaderUnitTest {

  private final String RECORD = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"009221\"}, { \"042\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"3\": \"test\" } ] } }, { \"042\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"pcc\" } ] } }, { \"042\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"pcc\" } ] } }, { \"245\":\"American Bar Association journal\" } ] }";
  private final String RECORD_WITH_DATE_DATA = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ {\"902\": {\"ind1\": \" \", \"ind2\": \" \", \"subfields\": [{\"a\": \"27-05-2020\"}, {\"b\": \"5\\/27\\/2020\"}, {\"c\": \"27.05.2020\"}, {\"d\": \"2020-05-27\"}]}} ] }";
  private final String RECORD_WITH_MULTIPLE_856 = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"009221\"},   {\"856\": { \"ind1\": \"4\", \"ind2\": \"0\", \"subfields\": [ { \"u\": \"https://fod.infobase.com\" }, { \"z\": \"image\" } ] }}, {\"856\": {\"ind1\": \"4\", \"ind2\": \"2\", \"subfields\": [{ \"u\": \"https://cfvod.kaltura.com\" }, { \"z\": \"films collection\" }]} }]}";
  private final String RECORD_WITH_MULTIPLE_876 = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"009221\"},   {\"876\": { \"ind1\": \"4\", \"ind2\": \"0\", \"subfields\": [ { \"n\": \"This is a binding note\" }, { \"t\": \"Binding\" } ] }}, {\"876\": {\"ind1\": \"4\", \"ind2\": \"2\", \"subfields\": [{ \"n\": \"This is an electronic bookplate note\" }, { \"t\": \"Electronic bookplate\" }]} }]}";
  private final String RECORD_WITHOUT_SUBFIELD_856_U = "{\"leader\": \"01314nam  22003851a 4500\", \"fields\": [{\"001\": \"009221\"}, {\"856\": {\"ind1\": \"4\", \"ind2\": \"0\", \"subfields\": [{\"z\": \"image\"}]}}]}";
  private final String RECORD_WITH_049 = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"009221\"},{\"048\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"https://fod.infobase.com\"},{\"z\":\"image\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"KU/CC/DI/M\"},{\"z\":\"Testing data\"}]}}]}";
  private final String RECORD_WITH_049_AND_BRACKETS = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"009221\"},{\"048\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"https://fod.infobase.com\"},{\"z\":\"image\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"(KU/CC/DI/M)\"},{\"z\":\"Testing data\"}]}}]}";
  private final String RECORD_WITH_049_AND_INVALID_BRACKETS = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"009221\"},{\"048\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"https://fod.infobase.com\"},{\"z\":\"image\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"K)U/CC(/D)I/M)\"},{\"z\":\"Testing data\"}]}}]}";
  private final String RECORD_WITH_049_WITH_OLI_LOCATION = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"009221\"},{\"048\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"https://fod.infobase.com\"},{\"z\":\"image\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"oli\"},{\"z\":\"Testing data\"}]}}]}";
  private final String RECORD_WITH_049_WITH_OLI_ALS_LOCATION = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"009221\"},{\"048\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"https://fod.infobase.com\"},{\"z\":\"image\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"oli,als\"},{\"z\":\"Testing data\"}]}}]}";
  private final String RECORD_WITH_049_WITH_OL_LOCATION = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"009221\"},{\"048\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"https://fod.infobase.com\"},{\"z\":\"image\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"ol\"},{\"z\":\"Testing data\"}]}}]}";

  private final String RECORD_WITH_MULTIPLE_028_FIELDS = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"009221\"},{\"028\":{\"ind1\":\"0\",\"ind2\":\"2\",\"subfields\":[{\"a\":\"MCA2-4047\"},{\"b\":\"bMCA Records\"}]}},{\"028\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"DXSB7-156\"},{\"b\":\"Decca\"}]}},{\"042\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"pcc\"}]}},{\"245\":\"American Bar Association journal\"}]}";

  private final String RECORD_WITH_MULTIPLE_028_FIELDS_2 = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"009221\"},{\"028\":{\"ind1\":\"0\",\"ind2\":\"2\",\"subfields\":[{\"a\":\"MCA2-4047\"},{\"b\":\"bMCA Records\"},{\"c\":\"Test1\"}]}},{\"028\":{\"ind1\":\"0\",\"ind2\":\"1\",\"subfields\":[{\"a\":\"DXSB7-156\"},{\"b\":\"Decca\"},{\"c\":\"Test2\"}]}},{\"028\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"DXSB7-157\"},{\"b\":\"Decca2\"},{\"c\":\"Test3\"}]}},{\"042\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"pcc\"}]}},{\"245\":\"American Bar Association journal\"}]}";
  private final String RECORD_WITH_SINGLE_028_FIELD = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"009221\"},{\"028\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"DXSB7-156\"},{\"b\":\"Decca\"}]}},{\"042\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"pcc\"}]}},{\"245\":\"American Bar Association journal\"}]}";

  private final String RECORD_WITH_THE_SAME_SUBFIELDS_IN_MULTIPLE_028_FIELDS = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"009221\"},{\"028\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"aT90028\"},{\"b\":\"Verve\"}]}},{\"028\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"aV-4061\"},{\"b\":\"Verve\"}]}},{\"042\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"pcc\"}]}},{\"245\":\"American Bar Association journal\"}]}";
  private final String RECORD_WITH_MULTIPLE_SUBFIELDS_IN_MULTIPLE_050_FIELD = "{\"leader\": \"01314nam  22003851a 4500\", \"fields\": [{\"001\": \"009221\"}, {\"050\": {\"ind1\": \"0\", \"ind2\": \"0\", \"subfields\": [{\"a\": \"Z2013.5.W6\"}, {\"b\": \"K46 2018\"}, {\"a\": \"PR1286.W6\"}]}}, {\"050\": {\"ind1\": \"0\", \"ind2\": \"0\", \"subfields\": [{\"a\": \"a2-val\"}, {\"b\": \"b2-val\"}, {\"a\": \"a2-val\"}]}}, {\"245\": \"American Bar Association journal\"}]}";

  private MappingContext mappingContext = new MappingContext();

  @Test
  public void shouldRead_Strings_FromRules() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule().withPath("").withValue("\"test\" \" \" \"value\""));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("test value", value.getValue());
  }

  @Test
  public void shouldRead_Marc_Leader() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule().withPath("").withValue("LDR/4"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("1", value.getValue());
  }

  @Test
  public void shouldRead_Marc_Leader_2() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule().withPath("").withValue("LDR/04"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("1", value.getValue());
  }

  @Test
  public void shouldRead_Marc_LeaderRange() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule().withPath("").withValue("LDR/4-5"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("14", value.getValue());
  }

  @Test
  public void shouldRead_Marc_LeaderRange_2() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule().withPath("").withValue("LDR/04-05"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("14", value.getValue());
  }

  @Test
  public void shouldRead_Marc_Controlled() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule().withPath("").withValue("001/4"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("2", value.getValue());
  }

  @Test
  public void shouldRead_Marc_Controlled_2() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule().withPath("").withValue("001/04"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("2", value.getValue());
  }

  @Test
  public void shouldRead_Marc_ControlledRange() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule().withPath("").withValue("001/4-5"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("22", value.getValue());
  }

  @Test
  public void shouldRead_Marc_ControlledRange_2() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule().withPath("").withValue("001/04-05"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("22", value.getValue());
  }

  @Test
  public void shouldRead_ArraysStrings_FromRules() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule().withPath("[]").withValue("\"test\" \" \" \"value\""));
    // then
    assertNotNull(value);
    assertEquals(ValueType.LIST, value.getType());
    assertEquals(Arrays.asList("test", "value"), value.getValue());
  }

  @Test
  public void shouldRead_ArraysStrings_FromRulesConditions() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule().withPath("[]").withValue("\" \";else \"value\""));
    // then
    assertNotNull(value);
    assertEquals(ValueType.LIST, value.getType());
    assertEquals(singletonList("value"), value.getValue());
  }

  @Test
  public void shouldRead_ArraysStrings_asMissing_FromRules() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule().withPath("[]").withValue("\" \""));
    // then
    assertNotNull(value);
    assertEquals(ValueType.MISSING, value.getType());
  }

  @Test
  public void shouldRead_AcceptedStrings_FromRules() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("randomUUID", "value");
    acceptedValues.put("randomUUID2", "noValue");
    // when
    Value value = reader.read(new MappingRule()
      .withPath("")
      .withValue("\"test\" \" \" \"value\" \" \"")
      .withAcceptedValues(acceptedValues));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("test randomUUID ", value.getValue());
  }

  @Test
  public void shouldRead_BooleanFields_FromRules() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    Value value = reader.read(new MappingRule()
      .withPath("")
      .withBooleanFieldAction(MappingRule.BooleanFieldAction.ALL_FALSE));
    // then
    assertNotNull(value);
    assertEquals(ValueType.BOOLEAN, value.getType());
    assertEquals(MappingRule.BooleanFieldAction.ALL_FALSE, value.getValue());
  }

  @Test
  public void shouldRead_MARCFields_FromRules() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    Value value = reader.read(new MappingRule()
      .withPath("")
      .withValue("042$a \" \" 042$a"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("pcc pcc", value.getValue());
  }

  @Test
  public void shouldRead_MARCFields_numeric_FromRules() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    Value value = reader.read(new MappingRule()
      .withPath("")
      .withValue("042$3 \" \" 042$a"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("test pcc", value.getValue());
  }

  @Test
  public void shouldRead_MARCFieldsArray_FromRules() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    Value value = reader.read(new MappingRule()
      .withPath("[]")
      .withValue("042$a \" \" 042$a"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.LIST, value.getType());
    List<String> result = new ArrayList<>();
    result.add("pcc");
    result.add("pcc");
    assertEquals(result, value.getValue());
  }

  @Test
  public void shouldRead_MARCFields_FromRulesWithConditions() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    Value value = reader.read(new MappingRule()
      .withPath("")
      .withValue("043$a \" \"; else 010; else 042$a \" \" \"data\" \" \" 001; else 042$a"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("pcc data 009221", value.getValue());
  }

  @Test
  public void shouldReadRulesWithWrongSyntax() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    Value value = reader.read(new MappingRule()
      .withPath("")
      .withValue("asd w3"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.MISSING, value.getType());
  }

  @Test
  public void shouldReadRepeatableFields() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    List<MappingRule> listRules = new ArrayList<>();
    List<MappingRule> listRules2 = new ArrayList<>();
    listRules.add(new MappingRule()
      .withName("name")
      .withPath("instance.name")
      .withEnabled("true")
      .withValue("043$a \" \"; else 010; else 042$a \" \" \"data\" \" \" 001; else 042$a")
    );
    listRules.add(new MappingRule()
      .withName("name")
      .withPath("instance.value")
      .withEnabled("true")
      .withValue("\"test\" \" \" \"value\""));
    listRules.add(new MappingRule()
      .withName("name")
      .withPath("instance.active")
      .withEnabled("true")
      .withBooleanFieldAction(MappingRule.BooleanFieldAction.ALL_FALSE));
    listRules2.add(new MappingRule()
      .withName("name")
      .withPath("instance.value")
      .withEnabled("true")
      .withValue("\"test\" \" \" \"value\""));

    Value value = reader.read(new MappingRule()
      .withPath("instance")
      .withRepeatableFieldAction(EXTEND_EXISTING)
      .withSubfields(Arrays.asList(new RepeatableSubfieldMapping()
        .withOrder(0)
        .withPath("instance")
        .withFields(listRules), new RepeatableSubfieldMapping()
        .withOrder(1)
        .withPath("instance")
        .withFields(listRules2)
      )));

    assertNotNull(value);
    assertEquals(ValueType.REPEATABLE, value.getType());
    assertEquals("instance", ((RepeatableFieldValue) value).getRootPath());
    assertEquals(EXTEND_EXISTING, ((RepeatableFieldValue) value).getRepeatableFieldAction());

    Map<String, Value> object1 = new HashMap<>();
    object1.put("instance.name", StringValue.of("pcc data 009221"));
    object1.put("instance.value", StringValue.of("test value"));
    object1.put("instance.active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    Map<String, Value> object2 = new HashMap<>();
    object2.put("instance.value", StringValue.of("test value"));

    assertEquals(JsonObject.mapFrom(RepeatableFieldValue.of(Arrays.asList(object1, object2), EXTEND_EXISTING, "instance")), JsonObject.mapFrom(value));
  }

  @Test
  public void shouldReadRepeatableFieldAndCreateFieldItemPerEverySpecifiedField() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_MULTIPLE_856))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    List<MappingRule> listRules = new ArrayList<>();

    listRules.add(new MappingRule()
      .withName("uri")
      .withPath("holdings.electronicAccess[].uri")
      .withEnabled("true")
      .withValue("856$u"));
    listRules.add(new MappingRule()
      .withName("relationshipId")
      .withPath("holdings.electronicAccess[].relationshipId")
      .withEnabled("true")
      .withValue("\"f5d0068e-6272-458e-8a81-b85e7b9a14aa\""));
    listRules.add(new MappingRule()
      .withName("linkText")
      .withPath("holdings.electronicAccess[].linkText")
      .withEnabled("true")
      .withValue("856$z"));

    Value value = reader.read(new MappingRule()
      .withName("electronicAccess")
      .withPath("holdings")
      .withRepeatableFieldAction(EXTEND_EXISTING)
      .withSubfields(singletonList(new RepeatableSubfieldMapping()
        .withOrder(0)
        .withPath("holdings.electronicAccess[]")
        .withFields(listRules))));

    assertNotNull(value);
    assertEquals(ValueType.REPEATABLE, value.getType());
    assertEquals("holdings", ((RepeatableFieldValue) value).getRootPath());
    assertEquals(EXTEND_EXISTING, ((RepeatableFieldValue) value).getRepeatableFieldAction());

    Map<String, Value> object1 = new HashMap<>();
    object1.put("holdings.electronicAccess[].uri", StringValue.of("https://fod.infobase.com"));
    object1.put("holdings.electronicAccess[].relationshipId", StringValue.of("f5d0068e-6272-458e-8a81-b85e7b9a14aa"));
    object1.put("holdings.electronicAccess[].linkText", StringValue.of("image"));

    Map<String, Value> object2 = new HashMap<>();
    object2.put("holdings.electronicAccess[].uri", StringValue.of("https://cfvod.kaltura.com"));
    object2.put("holdings.electronicAccess[].relationshipId", StringValue.of("f5d0068e-6272-458e-8a81-b85e7b9a14aa"));
    object2.put("holdings.electronicAccess[].linkText", StringValue.of("films collection"));

    assertEquals(JsonObject.mapFrom(RepeatableFieldValue.of(Arrays.asList(object1, object2), EXTEND_EXISTING, "holdings")), JsonObject.mapFrom(value));
  }

  @Test
  public void shouldReadRepeatableFieldWithAcceptedValues() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_MULTIPLE_876))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("UUID1", "Binding");
    acceptedValues.put("UUID2", "Electronic bookplate");

    List<MappingRule> listRules = new ArrayList<>();

    listRules.add(new MappingRule()
      .withName("itemNoteTypeId")
      .withPath("item.notes[].itemNoteTypeId")
      .withEnabled("true")
      .withValue("876$t")
      .withAcceptedValues(acceptedValues));
    listRules.add(new MappingRule()
      .withName("note")
      .withPath("item.notes[].note")
      .withEnabled("true")
      .withValue("876$n"));
    listRules.add(new MappingRule()
      .withName("staffOnly")
      .withPath("item.notes[].staffOnly")
      .withEnabled("true")
      .withBooleanFieldAction(MappingRule.BooleanFieldAction.ALL_TRUE));

    Value value = reader.read(new MappingRule()
      .withName("notes")
      .withPath("item.notes[]")
      .withRepeatableFieldAction(EXTEND_EXISTING)
      .withSubfields(singletonList(new RepeatableSubfieldMapping()
        .withOrder(0)
        .withPath("item.notes[]")
        .withFields(listRules))));

    assertNotNull(value);
    assertEquals(ValueType.REPEATABLE, value.getType());
    assertEquals("item.notes[]", ((RepeatableFieldValue) value).getRootPath());
    assertEquals(EXTEND_EXISTING, ((RepeatableFieldValue) value).getRepeatableFieldAction());

    Map<String, Value> object1 = new HashMap<>();
    object1.put("item.notes[].itemNoteTypeId", StringValue.of("UUID1"));
    object1.put("item.notes[].note", StringValue.of("This is a binding note"));
    object1.put("item.notes[].staffOnly", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_TRUE));

    Map<String, Value> object2 = new HashMap<>();
    object2.put("item.notes[].itemNoteTypeId", StringValue.of("UUID2"));
    object2.put("item.notes[].note", StringValue.of("This is an electronic bookplate note"));
    object2.put("item.notes[].staffOnly", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_TRUE));

    assertEquals(JsonObject.mapFrom(RepeatableFieldValue.of(Arrays.asList(object1, object2), EXTEND_EXISTING, "item.notes[]")), JsonObject.mapFrom(value));
  }

  @Test
  public void shouldReturnEmptyRepeatableFieldValueWhenHasNoDataForRequiredFieldUri() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITHOUT_SUBFIELD_856_U))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    List<MappingRule> listRules = new ArrayList<>();

    listRules.add(new MappingRule()
      .withName("uri")
      .withPath("holdings.electronicAccess[].uri")
      .withEnabled("true")
      .withValue("856$u"));
    listRules.add(new MappingRule()
      .withName("relationshipId")
      .withPath("holdings.electronicAccess[].relationshipId")
      .withEnabled("true")
      .withValue("\"f5d0068e-6272-458e-8a81-b85e7b9a14aa\""));
    listRules.add(new MappingRule()
      .withName("linkText")
      .withPath("holdings.electronicAccess[].linkText")
      .withEnabled("true")
      .withValue("856$z"));

    Value value = reader.read(new MappingRule()
      .withName("electronicAccess")
      .withPath("holdings")
      .withRepeatableFieldAction(EXTEND_EXISTING)
      .withSubfields(singletonList(new RepeatableSubfieldMapping()
        .withOrder(0)
        .withPath("holdings.electronicAccess[]")
        .withFields(listRules))));

    assertNotNull(value);
    assertEquals(ValueType.REPEATABLE, value.getType());
    assertTrue(((RepeatableFieldValue) value).getValue().isEmpty());
  }

  @Test
  public void shouldReadRepeatableFieldsIfSubfieldsAreEmptyAndActionIsDeleteExisting() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    Value value = reader.read(new MappingRule()
      .withPath("instance")
      .withRepeatableFieldAction(DELETE_EXISTING)
      .withSubfields(Collections.emptyList()));

    assertNotNull(value);
    assertEquals(ValueType.REPEATABLE, value.getType());
    assertEquals("instance", ((RepeatableFieldValue) value).getRootPath());
    assertEquals(DELETE_EXISTING, ((RepeatableFieldValue) value).getRepeatableFieldAction());

    assertEquals(JsonObject.mapFrom(RepeatableFieldValue.of(emptyList(), DELETE_EXISTING, "instance")), JsonObject.mapFrom(value));
  }

  @Test
  public void shouldReadRepeatableFieldsIfSubfieldsAreEmptyAndActionIsEmpty() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    Value value = reader.read(new MappingRule()
      .withPath("instance")
      .withRepeatableFieldAction(null)
      .withSubfields(Collections.emptyList()));

    assertNotNull(value);
    assertEquals(ValueType.MISSING, value.getType());
  }

  @Test
  public void shouldReadMARCFieldsFromRulesWithTodayExpression() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    String expectedDateString = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

    Value value = reader.read(new MappingRule()
      .withPath("")
      .withValue("902$a; else ###TODAY###"));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals(expectedDateString, value.getValue());
  }

  @Test
  public void shouldReadMARCFieldsFromRulesWithTodayExpressionWithoutTenantConfiguration() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);

    MappingContext mappingContext = new MappingContext()
      .withMappingParameters(new MappingParameters().withInitializedState(true));

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    String expectedDateString = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

    Value value = reader.read(new MappingRule()
      .withPath("")
      .withValue("902$a; else ###TODAY###"));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals(expectedDateString, value.getValue());
  }

  @Test
  public void shouldReadMARCFieldsFromRulesWithTodayExpressionAndTenantConfigurationWithDayDifferenceLessThan2days() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());

    MappingContext mappingContext = new MappingContext();
    mappingContext.setMappingParameters(new MappingParameters()
      .withInitializedState(true)
      .withTenantConfiguration("{\"locale\":\"en-US\",\"timezone\":\"Pacific/Kiritimati\",\"currency\":\"USD\"}"));

    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    String expectedDateString = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

    Value value = reader.read(new MappingRule()
      .withPath("")
      .withValue("902$a; else ###TODAY###"));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    LocalDate expectedDate = LocalDate.parse(expectedDateString, formatter);
    LocalDate actualDate = LocalDate.parse(String.valueOf(value.getValue()), formatter);
    Period age = Period.between(expectedDate, actualDate);
    int days = age.getDays();
    assertTrue(days < 2);
  }

  @Test
  public void shouldNotReadMARCFieldsFromRulesWithTodayExpressionAndInvalidTimezone() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);

    MappingContext mappingContext = new MappingContext().withMappingParameters(new MappingParameters()
      .withTenantConfiguration("{\"locale\":\"en-US\",\"timezone\":\"asdas/sadas\",\"currency\":\"USD\"}")
      .withInitializedState(true));

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    Value value = reader.read(new MappingRule()
      .withPath("")
      .withValue("902$a; else ###TODAY###"));
    assertNotNull(value);

    assertEquals(ValueType.MISSING, value.getType());
  }


  @Test
  public void shouldRead_MARCFieldsArrayAndFormatToISOFormat() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_DATE_DATA))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule()
      .withPath("[]")
      .withValue("902$a 902$b 902$c 902$d"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.LIST, value.getType());
    ((ListValue) value).getValue().forEach(s -> {
      assertEquals("2020-05-27", s);
    });
  }

  @Test
  public void shouldRead_MARCFieldsArrayWithRepeatableFieldAction_FromRules() throws IOException {
    // given
    List<String> expectedFields = Arrays.asList("UUID2", "UUID3");
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("UUID1", "website");
    acceptedValues.put("UUID2", "school program");
    acceptedValues.put("UUID3", "literature report");

    MappingRule fieldRule1 = new MappingRule()
      .withName("natureOfContentTermIds")
      .withPath("instance.natureOfContentTermIds[]")
      .withEnabled("true")
      .withValue("\"school program\"")
      .withAcceptedValues(acceptedValues);
    MappingRule fieldRule2 = new MappingRule()
      .withName("natureOfContentTermIds")
      .withPath("instance.natureOfContentTermIds[]")
      .withEnabled("true")
      .withValue("\"literature report\"")
      .withAcceptedValues(acceptedValues);

    MappingRule mappingRule = new MappingRule()
      .withPath("instance.natureOfContentTermIds[]")
      .withRepeatableFieldAction(EXTEND_EXISTING)
      .withSubfields(Arrays.asList(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("instance.natureOfContentTermIds[]")
          .withFields(singletonList(fieldRule1)),
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("instance.natureOfContentTermIds[]")
          .withFields(singletonList(fieldRule2))));

    // when
    Value value = reader.read(mappingRule);

    // then
    assertNotNull(value);
    assertEquals(ValueType.LIST, value.getType());
    assertEquals(EXTEND_EXISTING, ((ListValue) value).getRepeatableFieldAction());
    assertEquals(expectedFields, value.getValue());
  }

  @Test
  public void shouldRead_MARCFieldsArrayWithRepeatableFieldWithMARCValue_FromRules() throws IOException {
    // given
    List<String> expectedFields = Arrays.asList("pcc", "UUID3");
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("UUID1", "website");
    acceptedValues.put("UUID2", "school program");
    acceptedValues.put("UUID3", "literature report");

    MappingRule fieldRule1 = new MappingRule()
      .withName("formerIds")
      .withPath("instance.formerIds[]")
      .withEnabled("true")
      .withValue("042$a")
      .withAcceptedValues(acceptedValues);
    MappingRule fieldRule2 = new MappingRule()
      .withName("formerIds")
      .withPath("instance.formerIds[]")
      .withEnabled("true")
      .withValue("\"literature report\"")
      .withAcceptedValues(acceptedValues);

    MappingRule mappingRule = new MappingRule()
      .withPath("instance.formerIds[]")
      .withRepeatableFieldAction(EXTEND_EXISTING)
      .withSubfields(Arrays.asList(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("instance.formerIds[]")
          .withFields(singletonList(fieldRule1)),
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("instance.formerIds[]")
          .withFields(singletonList(fieldRule2))));

    // when
    Value value = reader.read(mappingRule);

    // then
    assertNotNull(value);
    assertEquals(ValueType.LIST, value.getType());
    assertEquals(EXTEND_EXISTING, ((ListValue) value).getRepeatableFieldAction());
    assertEquals(expectedFields, value.getValue());
  }

  @Test
  public void shouldReadRemoveExpressionFromRules() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    Value value = reader.read(new MappingRule()
      .withPath("catalogedDate")
      .withValue("###REMOVE###"));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertTrue(((StringValue) (value)).shouldRemoveOnWrite());
  }

  @Test
  public void shouldReadSpecificPermanentLocationWithBrackets() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_049))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    String expectedId = "fcd64ce1-6995-48f0-840e-89ffa2288371";
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Online (E)");
    acceptedValues.put(expectedId, "Main Library (KU/CC/DI/M)");
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822ffd", "ORWIG ETHNO CD (KU/CC/DI/O)");
    acceptedValues.put("f34d27c6-a8eb-461b-acd6-5dea81771e70", "SECOND FLOOR (KU/CC/DI/2)");

    Value value = reader.read(new MappingRule()
      .withPath("holdings.permanentLocationId")
      .withValue("049$a")
      .withAcceptedValues(acceptedValues));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals(expectedId, value.getValue());
  }

  @Test
  public void shouldReadEqualsPermanentLocationWithBracketsIfContainsSameCode() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_049))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    String expectedId = "fcd64ce1-6995-48f0-840e-89ffa2288371";
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822ffd", "ORWIG ETHNO CD (KU/CC/DI/MO)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Online (KU/CC/DI/MI)");
    acceptedValues.put(expectedId, "Main Library (KU/CC/DI/M)");
    acceptedValues.put("f34d27c6-a8eb-461b-acd6-5dea81771e70", "SECOND FLOOR (KU/CC/DI/MU)");

    Value value = reader.read(new MappingRule()
      .withPath("holdings.permanentLocationId")
      .withValue("049$a")
      .withAcceptedValues(acceptedValues));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals(expectedId, value.getValue());
  }

  @Test
  public void shouldNotReadPermanentLocationWithBracketsNotEqualsCode() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_049))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822ffd", "ORWIG ETHNO CD (KU/CC/DI/MO)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Online (KU/CC/DI/MI)");
    acceptedValues.put("fcd64ce1-6995-48f0-840e-89ffa2288371", "Main Library (KU/CC/DI/MK)");
    acceptedValues.put("f34d27c6-a8eb-461b-acd6-5dea81771e70", "SECOND FLOOR (KU/CC/DI/VU)");

    Value value = reader.read(new MappingRule()
      .withPath("holdings.permanentLocationId")
      .withValue("049$a")
      .withAcceptedValues(acceptedValues));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals("KU/CC/DI/M", value.getValue());
  }


  @Test
  public void shouldNotReadPermanentLocationWithoutBrackets() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_049))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822ffd", "ORWIG ETHNO CD (KU/CC/DI/MO)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Online (KU/CC/DI/MI)");
    acceptedValues.put("fcd64ce1-6995-48f0-840e-89ffa2288371", "Main Library KU/CC/DI/M");
    acceptedValues.put("f34d27c6-a8eb-461b-acd6-5dea81771e70", "SECOND FLOOR (KU/CC/DI/VU)");

    Value value = reader.read(new MappingRule()
      .withPath("holdings.permanentLocationId")
      .withValue("049$a")
      .withAcceptedValues(acceptedValues));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals("KU/CC/DI/M", value.getValue());
  }

  @Test
  public void shouldReadPermanentLocationIfRecordContainsBrackets() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_049_AND_BRACKETS))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822ffd", "ORWIG ETHNO CD (KU/CC/DI/MO)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Online (KU/CC/DI/MI)");
    acceptedValues.put("fcd64ce1-6995-48f0-840e-89ffa2288371", "Main Library (KU/CC/DI/M)");
    acceptedValues.put("f34d27c6-a8eb-461b-acd6-5dea81771e70", "SECOND FLOOR (KU/CC/DI/VU)");

    Value value = reader.read(new MappingRule()
      .withPath("holdings.permanentLocationId")
      .withValue("049$a")
      .withAcceptedValues(acceptedValues));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", value.getValue());
  }

  @Test
  public void shouldReadPermanentLocationFromTheLastBracketsEvenIfThereIsCommonValueInBracketsFromName() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_049_WITH_OLI_LOCATION))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822fff", "Oliss (oliss)");
    acceptedValues.put("f34d27c6-a8eb-461b-acd6-5dea81771e70", "SECOND FLOOR (KU/CC/DI/VU)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Ils ali (Oli) (oli,ils)");
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822ffd", "Oli (Oli) (oli)");
    acceptedValues.put("fcd64ce1-6995-48f0-840e-89ffa2288371", "Oli als (Oli)(oli,als)");

    Value value = reader.read(new MappingRule()
      .withPath("holdings.permanentLocationId")
      .withValue("049$a")
      .withAcceptedValues(acceptedValues));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals("758258bc-ecc1-41b8-abca-f7b610822ffd", value.getValue());
  }

  @Test
  public void shouldReadPermanentLocationFromTheLastBracketsWithSpecificLocation() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_049_WITH_OLI_ALS_LOCATION))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822fff", "Oliss (oliss)");
    acceptedValues.put("f34d27c6-a8eb-461b-acd6-5dea81771e70", "SECOND FLOOR (KU/CC/DI/VU)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Ils ali (Oli) (oli,ils)");
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822ffd", "Oli (Oli) (oli)");
    acceptedValues.put("fcd64ce1-6995-48f0-840e-89ffa2288371", "Oli als (Oli) (oli,als)");

    Value value = reader.read(new MappingRule()
      .withPath("holdings.permanentLocationId")
      .withValue("049$a")
      .withAcceptedValues(acceptedValues));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", value.getValue());
  }

  @Test
  public void shouldReadPermanentLocationFromTheLastBracketsEvenIfThereMoreThan2() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_049_WITH_OLI_LOCATION))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822fff", "Oliss (oliss) (oli) (ollll) (olls)");
    acceptedValues.put("f34d27c6-a8eb-461b-acd6-5dea81771e70", "SECOND FLOOR (KU/CC/DI/VU)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Ils ali (Oli) (oli))");
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822ffd", "Oli (Oli) (oli)");
    acceptedValues.put("fcd64ce1-6995-48f0-840e-89ffa2288371", "Oli als (Oli)(oli,als)");

    Value value = reader.read(new MappingRule()
      .withPath("holdings.permanentLocationId")
      .withValue("049$a")
      .withAcceptedValues(acceptedValues));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals("758258bc-ecc1-41b8-abca-f7b610822ffd", value.getValue());
  }

  @Test
  public void shouldNotReadPermanentLocationEvenIfNameContainsBracketsButNotEquals() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_049_WITH_OL_LOCATION))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822fff", "Oliss (oliss)");
    acceptedValues.put("f34d27c6-a8eb-461b-acd6-5dea81771e70", "SECOND FLOOR (KU/CC/DI/VU)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Ils ali (Oli) (ol) (oli,ils)");
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822ffd", "Oli (Oli) (oli)");
    acceptedValues.put("fcd64ce1-6995-48f0-840e-89ffa2288371", "Oli als (Oli) (oli,als)");

    Value value = reader.read(new MappingRule()
      .withPath("holdings.permanentLocationId")
      .withValue("049$a")
      .withAcceptedValues(acceptedValues));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals("ol", value.getValue());
  }

  @Test
  public void shouldNotReadPermanentLocationWhenRecordContainsInvalidBrackets() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_049_AND_INVALID_BRACKETS))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("758258bc-ecc1-41b8-abca-f7b610822ffd", "ORWIG ETHNO CD (KU/CC/DI/MO)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Online (KU/CC/DI/MI)");
    acceptedValues.put("fcd64ce1-6995-48f0-840e-89ffa2288371", "Main Library KU/CC/DI/M");
    acceptedValues.put("f34d27c6-a8eb-461b-acd6-5dea81771e70", "SECOND FLOOR (KU/CC/DI/VU)");

    Value value = reader.read(new MappingRule()
      .withPath("holdings.permanentLocationId")
      .withValue("049$a")
      .withAcceptedValues(acceptedValues));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals("K)U/CC(/D)I/M)", value.getValue());
  }

  @Test
  public void shouldRead_OrderComplexField() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("db9f5d17-0ca3-4d14-ae49-16b63c8fc084", "suf");
    acceptedValues.put("db9f5d17-0ca3-4d14-ae49-16b63c8fc083", "pref");

    Value value = reader.read(new MappingRule()
      .withName("prefix")
      .withPath("order.po.poNumberPrefix")
      .withEnabled("true")
      .withValue("\"pref\"")
      .withAcceptedValues(acceptedValues));

    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("db9f5d17-0ca3-4d14-ae49-16b63c8fc083", value.getValue());
  }

  @Test
  public void shouldRead_OrderArrayNonRepeatableField() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("0ebb1f7d-983f-3026-8a4c-5318e0ebc042", "online");
    acceptedValues.put("0ebb1f7d-983f-3026-8a4c-5318e0ebc041", "main");

    Value value = reader.read(new MappingRule()
      .withName("acqUnitIds")
      .withPath("order.po.acqUnitIds[]")
      .withEnabled("true")
      .withValue("\"main\"")
      .withAcceptedValues(acceptedValues));

    // then
    assertNotNull(value);
    assertEquals(ValueType.LIST, value.getType());
    assertEquals("[0ebb1f7d-983f-3026-8a4c-5318e0ebc041]", Lists.newArrayList(value.getValue()).get(0).toString());
  }

  @Test
  public void shouldRead_OrderLineComplexField() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    // when
    Value value = reader.read(new MappingRule()
      .withName("currency")
      .withPath("order.poLine.cost.currency")
      .withEnabled("true")
      .withValue("\"UAH\""));

    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("UAH", value.getValue());
  }

  @Test
  public void shouldRead_MARCFieldAsMissingValueIfMappingRulesNeedsToBeValidByAcceptedValuesAndIsNotValid() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    String uuid = "UUID";

    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put(uuid, String.format("CODE (%s)", uuid));
    MappingRule vendorRule = new MappingRule()
      .withName("vendor")
      .withPath("order.po.vendor")
      .withEnabled("true")
      .withValue("\"RANDOM\"")
      .withAcceptedValues(acceptedValues);
    MappingRule materialSupplierRule = new MappingRule()
      .withName("materialSupplier")
      .withPath("order.poLine.physical.materialSupplier")
      .withEnabled("true")
      .withValue("\"RANDOM\"")
      .withAcceptedValues(acceptedValues);
    MappingRule accessProviderRule = new MappingRule()
      .withName("accessProvider")
      .withPath("order.poLine.eresource.accessProvider")
      .withEnabled("true")
      .withValue("\"RANDOM\"")
      .withAcceptedValues(acceptedValues);

    // when
    Value valueVendor = reader.read(vendorRule);
    Value valueMaterialSupplier = reader.read(materialSupplierRule);
    Value valueAccessProvider = reader.read(accessProviderRule);

    // then
    assertNotNull(valueVendor);
    assertEquals(valueVendor.getType(), ValueType.MISSING);

    assertNotNull(valueMaterialSupplier);
    assertEquals(valueMaterialSupplier.getType(), ValueType.MISSING);

    assertNotNull(valueAccessProvider);
    assertEquals(valueAccessProvider.getType(), ValueType.MISSING);
  }

  @Test
  public void shouldRead_MARCFieldIfMappingRulesNeedsToBeValidByAcceptedValuesAndIsValid() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    String uuid = "UUID";

    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put(uuid, String.format("CODE (%s)", uuid));
    MappingRule vendorRule = new MappingRule()
      .withName("vendor")
      .withPath("order.po.vendor")
      .withEnabled("true")
      .withValue("\"CODE\"")
      .withAcceptedValues(acceptedValues);
    MappingRule materialSupplierRule = new MappingRule()
      .withName("materialSupplier")
      .withPath("order.poLine.physical.materialSupplier")
      .withEnabled("true")
      .withValue("\"CODE\"")
      .withAcceptedValues(acceptedValues);
    MappingRule accessProviderRule = new MappingRule()
      .withName("accessProvider")
      .withPath("order.poLine.eresource.accessProvider")
      .withEnabled("true")
      .withValue("\"CODE\"")
      .withAcceptedValues(acceptedValues);

    // when
    Value valueVendor = reader.read(vendorRule);
    Value valueMaterialSupplier = reader.read(materialSupplierRule);
    Value valueAccessProvider = reader.read(accessProviderRule);

    // then
    assertNotNull(valueVendor);
    assertEquals(valueVendor.getValue(), uuid);

    assertNotNull(valueMaterialSupplier);
    assertEquals(valueMaterialSupplier.getValue(), uuid);

    assertNotNull(valueAccessProvider);
    assertEquals(valueAccessProvider.getValue(), uuid);
  }

  @Test
  public void shouldReturnEmptyRepeatableFieldValueWhenHasNoDataForRequiredFieldProductId() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    MappingRule productIdRule = new MappingRule()
      .withName("productIds")
      .withPath("order.poLine.details.productIds[]")
      .withEnabled("true")
      .withValue("")
      .withRepeatableFieldAction(EXTEND_EXISTING)
      .withSubfields(List.of(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("order.poLine.details.productIds[]")
          .withFields(List.of(
            new MappingRule()
              .withName("productId")
              .withPath("order.poLine.details.productIds[].productId")
              .withValue("020$a")
              .withRequired(true)
              .withEnabled("true"),
            new MappingRule()
              .withName("qualifier")
              .withPath("order.poLine.details.productIds[].qualifier")
              .withValue("020$q")
              .withEnabled("true"),
            new MappingRule()
              .withName("productIdType")
              .withPath("order.poLine.details.productIds[].productIdType")
              .withValue("\"ISBN\"")
              .withEnabled("true")
            )
          )
      ));

    // when
    Value valueProductId = reader.read(productIdRule);

    // then
    assertNotNull(valueProductId);
    assertEquals(ValueType.REPEATABLE, valueProductId.getType());
    assertTrue(((RepeatableFieldValue) valueProductId).getValue().isEmpty());
  }

  @Test
  public void shouldReturnRepeatableFieldValueWhenHasNoDataForNotRequiredFieldProductId() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    MappingRule productIdRule = new MappingRule()
      .withName("productIds")
      .withPath("order.poLine.details.productIds[]")
      .withEnabled("true")
      .withValue("")
      .withRepeatableFieldAction(EXTEND_EXISTING)
      .withSubfields(List.of(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("order.poLine.details.productIds[]")
          .withFields(List.of(
              new MappingRule()
                .withName("productId")
                .withPath("order.poLine.details.productIds[].productId")
                .withValue("020$a")
                .withRequired(false)
                .withEnabled("true"),
              new MappingRule()
                .withName("qualifier")
                .withPath("order.poLine.details.productIds[].qualifier")
                .withValue("020$q")
                .withEnabled("true"),
              new MappingRule()
                .withName("productIdType")
                .withPath("order.poLine.details.productIds[].productIdType")
                .withValue("\"ISBN\"")
                .withEnabled("true")
            )
          )
      ));

    // when
    Value valueProductId = reader.read(productIdRule);

    // then
    assertNotNull(valueProductId);
    assertEquals(ValueType.REPEATABLE, valueProductId.getType());
    assertFalse(((RepeatableFieldValue) valueProductId).getValue().isEmpty());
  }

  @Test
  public void shouldReadAndConcatenate2Multiple028Fields() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_MULTIPLE_028_FIELDS))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    List<MappingRule> listRules = new ArrayList<>();
    listRules.add(new MappingRule()
      .withName("productId")
      .withPath("order.poLine.details.productIds[].productId")
      .withEnabled("true")
      .withValue("028$a \" \" 028$b")
    );

    Value value = reader.read(new MappingRule()
      .withPath("order.poLine.details.productIds[]")
      .withRepeatableFieldAction(EXTEND_EXISTING)
      .withSubfields(singletonList(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("order.poLine.details.productIds[]")
          .withFields(listRules)
      )));

    assertNotNull(value);
    assertEquals(ValueType.REPEATABLE, value.getType());
    assertEquals("order.poLine.details.productIds[]", ((RepeatableFieldValue) value).getRootPath());
    assertEquals(EXTEND_EXISTING, ((RepeatableFieldValue) value).getRepeatableFieldAction());

    Map<String, Value> object1 = new HashMap<>();
    object1.put("order.poLine.details.productIds[].productId", StringValue.of("MCA2-4047 bMCA Records"));
    Map<String, Value> object2 = new HashMap<>();
    object2.put("order.poLine.details.productIds[].productId", StringValue.of("DXSB7-156 Decca"));


    assertEquals(JsonObject.mapFrom(RepeatableFieldValue.of(Arrays.asList(object1, object2), EXTEND_EXISTING, "order.poLine.details.productIds[]")), JsonObject.mapFrom(value));
  }

  @Test
  public void shouldReadAndConcatenate3Multiple028Fields() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_MULTIPLE_028_FIELDS_2))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    List<MappingRule> listRules = new ArrayList<>();
    listRules.add(new MappingRule()
      .withName("productId")
      .withPath("order.poLine.details.productIds[].productId")
      .withEnabled("true")
      .withValue("028$a \" \" 028$b \" \" 028$c")
    );

    Value value = reader.read(new MappingRule()
      .withPath("order.poLine.details.productIds[]")
      .withRepeatableFieldAction(EXTEND_EXISTING)
      .withSubfields(singletonList(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("order.poLine.details.productIds[]")
          .withFields(listRules)
      )));

    assertNotNull(value);
    assertEquals(ValueType.REPEATABLE, value.getType());
    assertEquals("order.poLine.details.productIds[]", ((RepeatableFieldValue) value).getRootPath());
    assertEquals(EXTEND_EXISTING, ((RepeatableFieldValue) value).getRepeatableFieldAction());

    Map<String, Value> object1 = new HashMap<>();
    object1.put("order.poLine.details.productIds[].productId", StringValue.of("MCA2-4047 bMCA Records Test1"));
    Map<String, Value> object2 = new HashMap<>();
    object2.put("order.poLine.details.productIds[].productId", StringValue.of("DXSB7-156 Decca Test2"));
    Map<String, Value> object3 = new HashMap<>();
    object3.put("order.poLine.details.productIds[].productId", StringValue.of("DXSB7-157 Decca2 Test3"));


    assertEquals(JsonObject.mapFrom(RepeatableFieldValue.of(Arrays.asList(object1, object2, object3), EXTEND_EXISTING, "order.poLine.details.productIds[]")), JsonObject.mapFrom(value));
  }

  @Test
  public void shouldRead2Multiple028FieldsWithTheSameSubfield() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_THE_SAME_SUBFIELDS_IN_MULTIPLE_028_FIELDS))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    List<MappingRule> listRules = new ArrayList<>();
    listRules.add(new MappingRule()
      .withName("productId")
      .withPath("order.poLine.details.productIds[].productId")
      .withEnabled("true")
      .withValue("028$a \" \" 028$b")
    );

    Value value = reader.read(new MappingRule()
      .withPath("order.poLine.details.productIds[]")
      .withRepeatableFieldAction(EXTEND_EXISTING)
      .withSubfields(singletonList(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("order.poLine.details.productIds[]")
          .withFields(listRules)
      )));

    assertNotNull(value);
    assertEquals(ValueType.REPEATABLE, value.getType());
    assertEquals("order.poLine.details.productIds[]", ((RepeatableFieldValue) value).getRootPath());
    assertEquals(EXTEND_EXISTING, ((RepeatableFieldValue) value).getRepeatableFieldAction());

    Map<String, Value> object1 = new HashMap<>();
    object1.put("order.poLine.details.productIds[].productId", StringValue.of("aT90028 Verve"));
    Map<String, Value> object2 = new HashMap<>();
    object2.put("order.poLine.details.productIds[].productId", StringValue.of("aV-4061 Verve"));


    assertEquals(JsonObject.mapFrom(RepeatableFieldValue.of(Arrays.asList(object1, object2), EXTEND_EXISTING, "order.poLine.details.productIds[]")), JsonObject.mapFrom(value));
  }

  @Test
  public void shouldReturnMissingValueOnlyForRuleWithNullValueOnProcessingRepeatableFieldRule() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_MULTIPLE_028_FIELDS))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    String productIdFieldPath = "order.poLine.details.productIds[].productId";
    String productQualifierFieldPath = "order.poLine.details.productIds[].qualifier";

    List<MappingRule> productIdRules = List.of(
      new MappingRule()
        .withName("productId")
        .withPath(productIdFieldPath)
        .withEnabled("true")
        .withValue("028$a"),
      new MappingRule()
        .withName("qualifier")
        .withPath(productQualifierFieldPath)
        .withEnabled("true")
        .withValue(null));

    Value value = reader.read(new MappingRule()
      .withName("productIds")
      .withPath("order.poLine.details.productIds[]")
      .withRepeatableFieldAction(EXTEND_EXISTING)
      .withSubfields(List.of(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("order.poLine.details.productIds[]")
          .withFields(productIdRules)
      )));

    assertNotNull(value);
    assertEquals(ValueType.REPEATABLE, value.getType());
    RepeatableFieldValue actualValue = (RepeatableFieldValue) value;
    assertFalse(actualValue.getValue().isEmpty());
    assertEquals(ValueType.STRING, actualValue.getValue().get(0).get(productIdFieldPath).getType());
    assertEquals(ValueType.MISSING, actualValue.getValue().get(0).get(productQualifierFieldPath).getType());
  }

  @Test
  public void shouldReturnStringValueFromFirstSubfieldOnlyOnProcessingNonRepeatableFieldRuleWhenFieldHasMultipleSpecifiedSubfields() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD_WITH_MULTIPLE_SUBFIELDS_IN_MULTIPLE_050_FIELD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    MappingRule callNumberRule = new MappingRule()
      .withPath("holdings.callNumber")
      .withEnabled("true")
      .withValue("050$a");

    Value value = reader.read(callNumberRule);

    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("Z2013.5.W6", value.getValue());
  }

}
