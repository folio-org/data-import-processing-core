package org.folio.processing.mapping.reader;

import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.MarcBibReaderFactory;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnit4.class)
public class MarcRecordReaderUnitTest {
  private final String RECORD = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"009221\"},   { \"042\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"pcc\" } ] } }, { \"042\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"pcc\" } ] } }, { \"245\":\"American Bar Association journal\" } ] }";
  private final String RECORD_WITH_DATE_DATA = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ {\"902\": {\"ind1\": \" \", \"ind2\": \" \", \"subfields\": [{\"a\": \"27-05-2020\"}, {\"b\": \"5\\/27\\/2020\"}, {\"c\": \"27.05.2020\"}, {\"d\": \"2020-05-27\"}]}} ] }";

  @Test
  public void shouldRead_Strings_FromRules() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload);
    // when
    Value value = reader.read(new MappingRule().withPath("").withValue("\"test\" \" \" \"value\""));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("test value", value.getValue());
  }

  @Test
  public void shouldRead_ArraysStrings_FromRules() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload);
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
    reader.initialize(eventPayload);
    // when
    Value value = reader.read(new MappingRule().withPath("[]").withValue("\" \";else \"value\""));
    // then
    assertNotNull(value);
    assertEquals(ValueType.LIST, value.getType());
    assertEquals(Collections.singletonList("value"), value.getValue());
  }

  @Test
  public void shouldRead_ArraysStrings_asMissing_FromRules() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload);
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
    reader.initialize(eventPayload);
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
    reader.initialize(eventPayload);
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
    reader.initialize(eventPayload);
    Value value = reader.read(new MappingRule()
      .withPath("")
      .withValue("042$a \" \" 042$a"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.STRING, value.getType());
    assertEquals("pcc pcc", value.getValue());
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
    reader.initialize(eventPayload);
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
    reader.initialize(eventPayload);
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
    reader.initialize(eventPayload);
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
    reader.initialize(eventPayload);
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
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(Arrays.asList(new RepeatableSubfieldMapping()
        .withOrder(0)
        .withPath("instance")
        .withFields(listRules), new RepeatableSubfieldMapping()
        .withOrder(0)
        .withPath("instance")
        .withFields(listRules2)
      )));

    assertNotNull(value);
    assertEquals(ValueType.REPEATABLE, value.getType());
    assertEquals("instance", ((RepeatableFieldValue) value).getRootPath());
    assertEquals(MappingRule.RepeatableFieldAction.EXTEND_EXISTING, ((RepeatableFieldValue) value).getRepeatableFieldAction());

    Map<String, Value> object1 = new HashMap<>();
    object1.put("instance.name", StringValue.of("pcc data 009221"));
    object1.put("instance.value", StringValue.of("test value"));
    object1.put("instance.active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    Map<String, Value> object2 = new HashMap<>();
    object2.put("instance.value", StringValue.of("test value"));

    assertEquals(JsonObject.mapFrom(RepeatableFieldValue.of(Arrays.asList(object1, object2), MappingRule.RepeatableFieldAction.EXTEND_EXISTING, "instance")), JsonObject.mapFrom(value));
  }

  @Test
  public void shouldReadMARCFieldsFromRulesWithTodayExpression() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record()
      .withParsedRecord(new ParsedRecord().withContent(RECORD))).encode());
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload);
    String expectedDateString = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

    Value value = reader.read(new MappingRule()
      .withPath("")
      .withValue("902$a; else ###TODAY###"));
    assertNotNull(value);

    assertEquals(ValueType.STRING, value.getType());
    assertEquals(expectedDateString, value.getValue());
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
    reader.initialize(eventPayload);
    // when
    Value value = reader.read(new MappingRule()
      .withPath("[]")
      .withValue("902$a 902$b 902$c 902$d"));
    // then
    assertNotNull(value);
    assertEquals(ValueType.LIST, value.getType());
    ((ListValue)value).getValue().forEach(s -> {
      assertEquals("2020-05-27", s);
    });
  }
}
