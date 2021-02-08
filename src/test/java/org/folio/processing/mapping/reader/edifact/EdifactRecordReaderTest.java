package org.folio.processing.mapping.reader.edifact;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.processing.mapping.mapper.reader.record.edifact.EdifactReaderFactory;
import org.folio.processing.value.BooleanValue;
import org.folio.processing.value.RepeatableFieldValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.RepeatableSubfieldMapping;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;
import static org.folio.rest.jaxrs.model.MappingRule.BooleanFieldAction.ALL_TRUE;
import static org.folio.rest.jaxrs.model.MappingRule.RepeatableFieldAction.EXTEND_EXISTING;

public class EdifactRecordReaderTest {

  private static final String EDIFACT_PARSED_CONTENT = "{\"segments\" : [ {\"tag\" : \"UNA\", \"dataElements\" : [ ]}, {\"tag\" : \"UNB\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"UNOC\"}, {\"data\" : \"3\"} ]}, {\"components\" : [ {\"data\" : \"EBSCO\"}, {\"data\" : \"92\"} ]}, {\"components\" : [ {\"data\" : \"KOH0002\"}, {\"data\" : \"91\"} ]}, {\"components\" : [ {\"data\" : \"200610\"}, {\"data\" : \"0105\"} ]}, {\"components\" : [ {\"data\" : \"5162\"} ]} ]}, {\"tag\" : \"UNH\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"5162-1\"} ]}, {\"components\" : [ {\"data\" : \"INVOIC\"}, {\"data\" : \"D\"}, {\"data\" : \"96A\"}, {\"data\" : \"UN\"}, {\"data\" : \"EAN008\"} ]} ]}, {\"tag\" : \"BGM\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"380\"}, {\"data\" : \"\"}, {\"data\" : \"\"}, {\"data\" : \"JINV\"} ]}, {\"components\" : [ {\"data\" : \"0704159\"} ]}, {\"components\" : [ {\"data\" : \"43\"} ]} ]}, {\"tag\" : \"DTM\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"137\"}, {\"data\" : \"20191002\"}, {\"data\" : \"102\"} ]} ]}, {\"tag\" : \"NAD\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"BY\"} ]}, {\"components\" : [ {\"data\" : \"BR1624506\"}, {\"data\" : \"\"}, {\"data\" : \"91\"} ]} ]}, {\"tag\" : \"NAD\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"SR\"} ]}, {\"components\" : [ {\"data\" : \"EBSCO\"}, {\"data\" : \"\"}, {\"data\" : \"92\"} ]} ]}, {\"tag\" : \"CUX\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"2\"}, {\"data\" : \"USD\"}, {\"data\" : \"4\"} ]} ]}, {\"tag\" : \"LIN\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"1\"} ]} ]}, {\"tag\" : \"PIA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"5\"} ]}, {\"components\" : [ {\"data\" : \"004362033\"}, {\"data\" : \"SA\"} ]}, {\"components\" : [ {\"data\" : \"1941-6067\"}, {\"data\" : \"IS\"} ]} ]}, {\"tag\" : \"PIA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"5S\"} ]}, {\"components\" : [ {\"data\" : \"1941-6067(20200101)14;1-F\"}, {\"data\" : \"SI\"}, {\"data\" : \"\"}, {\"data\" : \"28\"} ]} ]}, {\"tag\" : \"PIA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"5E\"} ]}, {\"components\" : [ {\"data\" : \"1941-6067(20201231)14;1-F\"}, {\"data\" : \"SI\"}, {\"data\" : \"\"}, {\"data\" : \"28\"} ]} ]}, {\"tag\" : \"IMD\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"L\"} ]}, {\"components\" : [ {\"data\" : \"050\"} ]}, {\"components\" : [ {\"data\" : \"\"}, {\"data\" : \"\"}, {\"data\" : \"\"}, {\"data\" : \"ACADEMY OF MANAGEMENT ANNALS -   ON\"}, {\"data\" : \"LINE FOR INSTITUTIONS\"} ]} ]}, {\"tag\" : \"QTY\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"47\"}, {\"data\" : \"1\"} ]} ]}, {\"tag\" : \"DTM\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"194\"}, {\"data\" : \"20200101\"}, {\"data\" : \"102\"} ]} ]}, {\"tag\" : \"DTM\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"206\"}, {\"data\" : \"20201231\"}, {\"data\" : \"102\"} ]} ]}, {\"tag\" : \"MOA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"203\"}, {\"data\" : \"208.59\"}, {\"data\" : \"USD\"}, {\"data\" : \"4\"} ]} ]}, {\"tag\" : \"PRI\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"AAB\"}, {\"data\" : \"205\"} ]} ]}, {\"tag\" : \"RFF\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"LI\"}, {\"data\" : \"S255699\"} ]} ]}, {\"tag\" : \"RFF\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"SNA\"}, {\"data\" : \"C6546362\"} ]} ]}, {\"tag\" : \"ALC\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"C\"} ]}, {\"components\" : [ {\"data\" : \"\"} ]}, {\"components\" : [ {\"data\" : \"\"} ]}, {\"components\" : [ {\"data\" : \"\"} ]}, {\"components\" : [ {\"data\" : \"G74\"}, {\"data\" : \"\"}, {\"data\" : \"28\"}, {\"data\" : \"LINE SERVICE CHARGE\"} ]} ]}, {\"tag\" : \"MOA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"8\"}, {\"data\" : \"3.59\"} ]} ]}, {\"tag\" : \"LIN\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"2\"} ]} ]}, {\"tag\" : \"PIA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"5\"} ]}, {\"components\" : [ {\"data\" : \"006288237\"}, {\"data\" : \"SA\"} ]}, {\"components\" : [ {\"data\" : \"1944-737X\"}, {\"data\" : \"IS\"} ]} ]}, {\"tag\" : \"PIA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"5S\"} ]}, {\"components\" : [ {\"data\" : \"1944-737X(20200301)117;1-F\"}, {\"data\" : \"SI\"}, {\"data\" : \"\"}, {\"data\" : \"28\"} ]} ]}, {\"tag\" : \"PIA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"5E\"} ]}, {\"components\" : [ {\"data\" : \"1944-737X(20210228)118;1-F\"}, {\"data\" : \"SI\"}, {\"data\" : \"\"}, {\"data\" : \"28\"} ]} ]}, {\"tag\" : \"IMD\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"L\"} ]}, {\"components\" : [ {\"data\" : \"050\"} ]}, {\"components\" : [ {\"data\" : \"\"}, {\"data\" : \"\"}, {\"data\" : \"\"}, {\"data\" : \"ACI MATERIALS JOURNAL - ONLINE   -\"}, {\"data\" : \"MULTI USER\"} ]} ]}, {\"tag\" : \"QTY\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"47\"}, {\"data\" : \"1\"} ]} ]}, {\"tag\" : \"DTM\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"194\"}, {\"data\" : \"20200301\"}, {\"data\" : \"102\"} ]} ]}, {\"tag\" : \"DTM\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"206\"}, {\"data\" : \"20210228\"}, {\"data\" : \"102\"} ]} ]}, {\"tag\" : \"MOA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"203\"}, {\"data\" : \"726.5\"}, {\"data\" : \"USD\"}, {\"data\" : \"4\"} ]} ]}, {\"tag\" : \"PRI\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"AAB\"}, {\"data\" : \"714\"} ]} ]}, {\"tag\" : \"RFF\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"LI\"}, {\"data\" : \"S283902\"} ]} ]}, {\"tag\" : \"RFF\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"SNA\"}, {\"data\" : \"E9498295\"} ]} ]}, {\"tag\" : \"ALC\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"C\"} ]}, {\"components\" : [ {\"data\" : \"\"} ]}, {\"components\" : [ {\"data\" : \"\"} ]}, {\"components\" : [ {\"data\" : \"\"} ]}, {\"components\" : [ {\"data\" : \"G74\"}, {\"data\" : \"\"}, {\"data\" : \"28\"}, {\"data\" : \"LINE SERVICE CHARGE\"} ]} ]}, {\"tag\" : \"MOA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"8\"}, {\"data\" : \"12.5\"} ]} ]}, {\"tag\" : \"LIN\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"3\"} ]} ]}, {\"tag\" : \"PIA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"5\"} ]}, {\"components\" : [ {\"data\" : \"006289532\"}, {\"data\" : \"SA\"} ]}, {\"components\" : [ {\"data\" : \"1944-7361\"}, {\"data\" : \"IS\"} ]} ]}, {\"tag\" : \"PIA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"5S\"} ]}, {\"components\" : [ {\"data\" : \"1944-7361(20200301)117;1-F\"}, {\"data\" : \"SI\"}, {\"data\" : \"\"}, {\"data\" : \"28\"} ]} ]}, {\"tag\" : \"PIA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"5E\"} ]}, {\"components\" : [ {\"data\" : \"1944-7361(20210228)118;1-F\"}, {\"data\" : \"SI\"}, {\"data\" : \"\"}, {\"data\" : \"28\"} ]} ]}, {\"tag\" : \"IMD\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"L\"} ]}, {\"components\" : [ {\"data\" : \"050\"} ]}, {\"components\" : [ {\"data\" : \"\"}, {\"data\" : \"\"}, {\"data\" : \"\"}, {\"data\" : \"ACI STRUCTURAL JOURNAL -   ON\"}, {\"data\" : \"LINE - MULTI USER\"} ]} ]}, {\"tag\" : \"QTY\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"47\"}, {\"data\" : \"1\"} ]} ]}, {\"tag\" : \"DTM\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"194\"}, {\"data\" : \"20200301\"}, {\"data\" : \"102\"} ]} ]}, {\"tag\" : \"DTM\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"206\"}, {\"data\" : \"20210228\"}, {\"data\" : \"102\"} ]} ]}, {\"tag\" : \"MOA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"203\"}, {\"data\" : \"726.5\"}, {\"data\" : \"USD\"}, {\"data\" : \"4\"} ]} ]}, {\"tag\" : \"PRI\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"AAB\"}, {\"data\" : \"714\"} ]} ]}, {\"tag\" : \"RFF\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"LI\"}, {\"data\" : \"S283901\"} ]} ]}, {\"tag\" : \"RFF\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"SNA\"}, {\"data\" : \"E9498296\"} ]} ]}, {\"tag\" : \"ALC\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"C\"} ]}, {\"components\" : [ {\"data\" : \"\"} ]}, {\"components\" : [ {\"data\" : \"\"} ]}, {\"components\" : [ {\"data\" : \"\"} ]}, {\"components\" : [ {\"data\" : \"G74\"}, {\"data\" : \"\"}, {\"data\" : \"28\"}, {\"data\" : \"LINE SERVICE CHARGE\"} ]} ]}, {\"tag\" : \"MOA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"8\"}, {\"data\" : \"12.5\"} ]} ]}, {\"tag\" : \"UNS\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"S\"} ]} ]}, {\"tag\" : \"CNT\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"1\"}, {\"data\" : \"3\"} ]} ]}, {\"tag\" : \"CNT\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"2\"}, {\"data\" : \"3\"} ]} ]}, {\"tag\" : \"MOA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"79\"}, {\"data\" : \"18929.07\"} ]} ]}, {\"tag\" : \"MOA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"9\"}, {\"data\" : \"18929.07\"} ]} ]}, {\"tag\" : \"ALC\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"C\"} ]}, {\"components\" : [ {\"data\" : \"\"} ]}, {\"components\" : [ {\"data\" : \"\"} ]}, {\"components\" : [ {\"data\" : \"\"} ]}, {\"components\" : [ {\"data\" : \"G74\"}, {\"data\" : \"\"}, {\"data\" : \"28\"}, {\"data\" : \"TOTAL SERVICE CHARGE\"} ]} ]}, {\"tag\" : \"MOA\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"8\"}, {\"data\" : \"325.59\"} ]} ]}, {\"tag\" : \"UNT\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"294\"} ]}, {\"components\" : [ {\"data\" : \"5162-1\"} ]} ]}, {\"tag\" : \"UNZ\", \"dataElements\" : [ {\"components\" : [ {\"data\" : \"1\"} ]}, {\"components\" : [ {\"data\" : \"5162\"} ]} ]} ]}";

  private final ReaderFactory readerFactory = new EdifactReaderFactory();

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenPayloadHasNoRecord() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    dataImportEventPayload.setContext(new HashMap<>());

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenPayloadHasNoParsedRecordContentRecord() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord())));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);
  }

  @Test
  public void shouldReadStringConstantFromMappingRule() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);

    Value value = reader.read(new MappingRule().withPath("invoice.status").withValue("\"Open\""));

    Assert.assertEquals(Value.ValueType.STRING, value.getType());
    Assert.assertEquals("Open", value.getValue());
  }

  @Test
  public void shouldReturnStringValue() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);

    Value value = reader.read(new MappingRule().withPath("invoice.lockTotal").withValue("MOA+9[2]"));

    Assert.assertEquals(Value.ValueType.STRING, value.getType());
    Assert.assertEquals("18929.07", value.getValue());
  }

  @Test
  public void shouldReturnStringValueWhenMappingExpressionHasQualifier() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    // when
    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);
    Value value = reader.read(new MappingRule().withPath("invoice.lockTotal").withValue("CUX+2?4[2]"));

    // then
    Assert.assertEquals(Value.ValueType.STRING, value.getType());
    Assert.assertEquals("USD", value.getValue());
  }

  @Test
  public void shouldReadBooleanValueWhenMappingRuleHasBooleanFieldAction() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    // when
    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);
    Value value = reader.read(new MappingRule().withPath("invoice.chkSubscriptionOverlap").withBooleanFieldAction(ALL_TRUE));

    // then
    Assert.assertEquals(Value.ValueType.BOOLEAN, value.getType());
    Assert.assertEquals(ALL_TRUE, value.getValue());
  }

  @Test
  public void shouldReturnListValueWhenMappingRuleHasArrayFieldPath() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    HashMap<String, String> acqUnitsAcceptedValues = new HashMap<>(Map.of(
      "b2c0e100-0485-43f2-b161-3c60aac9f68a", "ackUnit-1",
      "b2c0e100-0485-43f2-b161-3c60aac9f128", "ackUnit-2",
      "b2c0e100-0485-43f2-b161-3c60aac9f256", "ackUnit-3"));

    MappingRule mappingRule = new MappingRule().withPath("invoice.acqUnitIds[]")
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(Arrays.asList(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("invoice.acqUnitIds[]")
          .withFields(singletonList(
            new MappingRule()
              .withPath("invoice.acqUnitIds[]")
              .withValue("\"ackUnit-1\"")
              .withAcceptedValues(acqUnitsAcceptedValues))),
        new RepeatableSubfieldMapping()
          .withOrder(1)
          .withPath("invoice.acqUnitIds[]")
          .withFields(singletonList(
            new MappingRule()
              .withPath("invoice.acqUnitIds[]")
              .withValue("\"ackUnit-2\"")
              .withAcceptedValues(acqUnitsAcceptedValues)))
      ));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);

    // when
    Value value = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.LIST, value.getType());
    Assert.assertEquals(Arrays.asList("b2c0e100-0485-43f2-b161-3c60aac9f68a", "b2c0e100-0485-43f2-b161-3c60aac9f128"), value.getValue());
  }

  @Test
  public void shouldReturnRepeatableFieldValue() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    HashMap<String, String> fundAcceptedValues = new HashMap<>(Map.of(
      "b2c0e100-0485-43f2-b161-3c60aac9f687", "fund-1",
      "b2c0e100-0485-43f2-b161-3c60aac9f177", "fund-2",
      "b2c0e100-0485-43f2-b161-3c60aac9f777", "fund-3"));

    String rootPath = "invoice.adjustments[]";
    String fundDistributionsRootPath = "invoice.adjustments[].fundDistributions[]";
    MappingRule mappingRule = new MappingRule().withPath(rootPath)
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(singletonList(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath(rootPath)
          .withFields(Arrays.asList(
            new MappingRule()
              .withPath("invoice.adjustments[].description")
              .withValue("\"description-1\""),
            new MappingRule()
              .withPath("invoice.adjustments[].exportToAccounting")
              .withBooleanFieldAction(ALL_TRUE),
            new MappingRule()
              .withPath("invoice.adjustments[].fundDistributions[]")
              .withRepeatableFieldAction(EXTEND_EXISTING)
              .withSubfields(singletonList(new RepeatableSubfieldMapping()
                .withOrder(0)
                .withPath(fundDistributionsRootPath)
                .withFields(Arrays.asList(
                  new MappingRule()
                    .withPath("invoice.adjustments[].fundDistributions[].fundId")
                    .withValue("\"fund-3\"")
                    .withAcceptedValues(fundAcceptedValues),
                  new MappingRule()
                    .withPath("invoice.adjustments[].fundDistributions[].code")
                    .withValue("\"USHIST\""))))))
          )));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);

    // when
    Value actualValue = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.REPEATABLE, actualValue.getType());
    RepeatableFieldValue repeatableFieldValue = (RepeatableFieldValue) actualValue;
    Assert.assertEquals(rootPath, repeatableFieldValue.getRootPath());
    Assert.assertEquals(EXTEND_EXISTING, repeatableFieldValue.getRepeatableFieldAction());

    Map<String, Value> expectedFundDistributionElement = Map.of(
      "invoice.adjustments[].fundDistributions[].fundId", StringValue.of("b2c0e100-0485-43f2-b161-3c60aac9f777"),
      "invoice.adjustments[].fundDistributions[].code", StringValue.of("USHIST"));

    Map<String, Value> expectedAdjustments = Map.of(
      "invoice.adjustments[].description", StringValue.of("description-1"),
      "invoice.adjustments[].exportToAccounting", BooleanValue.of(ALL_TRUE),
      "invoice.adjustments[].fundDistributions[]", RepeatableFieldValue.of(List.of(expectedFundDistributionElement), EXTEND_EXISTING, fundDistributionsRootPath));

    RepeatableFieldValue expectedValue = RepeatableFieldValue.of(List.of(expectedAdjustments), EXTEND_EXISTING, rootPath);
    Assert.assertEquals(JsonObject.mapFrom(expectedValue), JsonObject.mapFrom(actualValue));
  }

  @Test
  public void shouldReadMappingRuleWithElseClause() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    String expressionWithElseClause = "MOA+86[2]; else MOA+9[2]";
    String rootPath = "invoice.adjustments[]";
    String fundDistributionsRootPath = "invoice.adjustments[].fundDistributions[]";
    MappingRule mappingRule = new MappingRule().withPath(rootPath)
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(singletonList(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath(rootPath)
          .withFields(Arrays.asList(
            new MappingRule()
              .withPath("invoice.adjustments[].description")
              .withValue("\"test adjustment\""),
            new MappingRule()
              .withPath("invoice.adjustments[].fundDistributions[]")
              .withRepeatableFieldAction(EXTEND_EXISTING)
              .withSubfields(singletonList(new RepeatableSubfieldMapping()
                .withOrder(0)
                .withPath(fundDistributionsRootPath)
                .withFields(Arrays.asList(
                  new MappingRule()
                    .withPath("invoice.adjustments[].fundDistributions[].value")
                    .withValue(expressionWithElseClause),
                  new MappingRule()
                    .withPath("invoice.adjustments[].fundDistributions[].code")
                    .withValue("\"USHIST\""))))))
          )));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);

    // when
    Value actualValue = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.REPEATABLE, actualValue.getType());
    RepeatableFieldValue repeatableFieldValue = (RepeatableFieldValue) actualValue;
    Assert.assertEquals(rootPath, repeatableFieldValue.getRootPath());
    Assert.assertEquals(EXTEND_EXISTING, repeatableFieldValue.getRepeatableFieldAction());

    Map<String, Value> expectedFundDistributionElement = Map.of(
      "invoice.adjustments[].fundDistributions[].value", StringValue.of("18929.07"),
      "invoice.adjustments[].fundDistributions[].code", StringValue.of("USHIST"));

    Map<String, Value> expectedAdjustments = Map.of(
      "invoice.adjustments[].description", StringValue.of("test adjustment"),
      "invoice.adjustments[].fundDistributions[]", RepeatableFieldValue.of(List.of(expectedFundDistributionElement), EXTEND_EXISTING, fundDistributionsRootPath));

    RepeatableFieldValue expectedValue = RepeatableFieldValue.of(List.of(expectedAdjustments), EXTEND_EXISTING, rootPath);
    Assert.assertEquals(JsonObject.mapFrom(expectedValue), JsonObject.mapFrom(actualValue));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenMappingRuleHasInvalidMappingSyntax() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);
    reader.read(new MappingRule().withPath("invoice.status").withValue("bla expression"));
  }

  //
  @Test
  public void shouldReturnRepeatableFieldValueForInvoiceLineMappingRule() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    String rootPath = "invoice.invoiceLine[]";
    String adjustmentsPath = "invoice.invoiceLine[].adjustments[]";
    MappingRule mappingRule = new MappingRule().withPath(rootPath)
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(singletonList(new RepeatableSubfieldMapping()
        .withOrder(0)
        .withPath(rootPath)
        .withFields(Arrays.asList(
          new MappingRule()
            .withPath("invoice.invoiceLine[].invoiceLineStatus")
            .withValue("\"Open\""),
          new MappingRule()
            .withPath("invoice.invoiceLine[].description")
            .withValue("IMD+L+050+[4]"),
          new MappingRule()
            .withPath("invoice.invoiceLine[].vendorRefNo")
            .withValue("RFF+SNA[2]"),
          new MappingRule()
            .withPath(adjustmentsPath)
            .withRepeatableFieldAction(EXTEND_EXISTING)
            .withSubfields(singletonList(new RepeatableSubfieldMapping()
              .withOrder(0)
              .withPath(adjustmentsPath)
              .withFields(Arrays.asList(
                new MappingRule()
                  .withPath("invoice.invoiceLine[].adjustments[].description")
                  .withValue("ALC+C++++[4]"),
                new MappingRule()
                  .withPath("invoice.invoiceLine[].adjustments[].value")
                  .withValue("MOA+8[2]"),
                new MappingRule()
                  .withPath("invoice.invoiceLine[].adjustments[].exportToAccounting")
                  .withBooleanFieldAction(ALL_TRUE)))))
        ))));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);

    // when
    Value value = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.REPEATABLE, value.getType());
    RepeatableFieldValue actualValue = (RepeatableFieldValue) value;
    Assert.assertEquals(rootPath, actualValue.getRootPath());
    Assert.assertEquals(EXTEND_EXISTING, actualValue.getRepeatableFieldAction());

    Map<String, Value> expectedAdjustment1 = Map.of(
      "invoice.invoiceLine[].adjustments[].description", StringValue.of("LINE SERVICE CHARGE"),
      "invoice.invoiceLine[].adjustments[].value", StringValue.of("3.59"),
      "invoice.invoiceLine[].adjustments[].exportToAccounting", BooleanValue.of(ALL_TRUE));
    Map<String, Value> expectedAdjustment2 = Map.of(
      "invoice.invoiceLine[].adjustments[].description", StringValue.of("LINE SERVICE CHARGE"),
      "invoice.invoiceLine[].adjustments[].value", StringValue.of("12.5"),
      "invoice.invoiceLine[].adjustments[].exportToAccounting", BooleanValue.of(ALL_TRUE));
    Map<String, Value> expectedAdjustment3 = Map.of(
      "invoice.invoiceLine[].adjustments[].description", StringValue.of("LINE SERVICE CHARGE"),
      "invoice.invoiceLine[].adjustments[].value", StringValue.of("12.5"),
      "invoice.invoiceLine[].adjustments[].exportToAccounting", BooleanValue.of(ALL_TRUE));

    List<Map<String, Value>> expectedInvoiceLines = List.of(
      Map.of("invoice.invoiceLine[].description", StringValue.of("ACADEMY OF MANAGEMENT ANNALS -   ON"),
        "invoice.invoiceLine[].invoiceLineStatus", StringValue.of("Open"),
        "invoice.invoiceLine[].adjustments[]", RepeatableFieldValue.of(List.of(expectedAdjustment1), EXTEND_EXISTING, adjustmentsPath),
        "invoice.invoiceLine[].vendorRefNo", StringValue.of("C6546362")),
      Map.of("invoice.invoiceLine[].description", StringValue.of("ACI MATERIALS JOURNAL - ONLINE   -"),
        "invoice.invoiceLine[].invoiceLineStatus", StringValue.of("Open"),
        "invoice.invoiceLine[].adjustments[]", RepeatableFieldValue.of(List.of(expectedAdjustment2), EXTEND_EXISTING, adjustmentsPath),
        "invoice.invoiceLine[].vendorRefNo", StringValue.of("E9498295")),
      Map.of("invoice.invoiceLine[].description", StringValue.of("ACI STRUCTURAL JOURNAL -   ON"),
        "invoice.invoiceLine[].invoiceLineStatus", StringValue.of("Open"),
        "invoice.invoiceLine[].adjustments[]", RepeatableFieldValue.of(List.of(expectedAdjustment3), EXTEND_EXISTING, adjustmentsPath),
        "invoice.invoiceLine[].vendorRefNo", StringValue.of("E9498296")));

    RepeatableFieldValue expectedValue = RepeatableFieldValue.of(expectedInvoiceLines, EXTEND_EXISTING, rootPath);
    Assert.assertEquals(JsonObject.mapFrom(expectedValue), JsonObject.mapFrom(actualValue));
  }

  // todo:
  @Ignore
  @Test
  public void shouldReturnValueWhenSegmentPathHasMultipleCriteria() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);

    Value value = reader.read(new MappingRule().withPath("invoice_line.description").withValue("IMD+L+050+[4]"));

    Assert.assertEquals(Value.ValueType.STRING, value.getType());
    Assert.assertEquals("18929.07", value.getValue());
  }

  @Ignore
  @Test
  public void shouldReturnStringValueWhenMappingExpressionHasQualifier2() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload);

    Value value = reader.read(new MappingRule().withPath("invoice.lockTotal").withValue("MOA+203?4[2]"));

    // then
    Assert.assertEquals(Value.ValueType.STRING, value.getType());
    Assert.assertEquals("18929.07", value.getValue());
  }

}
