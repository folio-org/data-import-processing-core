package org.folio.processing.mapping.mapper;

import com.google.common.collect.Lists;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.mapper.mappers.ItemMapper;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.RepeatableSubfieldMapping;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.folio.processing.mapping.mapper.mappers.HoldingsMapper.MULTIPLE_HOLDINGS_FIELD;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(JUnit4.class)
public class ItemMapperTest {
  private final String PARSED_CONTENT_WITH_MULTIPLE_FIELDS = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"944\":{\"subfields\":[{\"s\":\"testCode2\"}],\"ind1\":\" \",\"ind2\":\" \"}}, {\"945\":{\"subfields\":[{\"a\":\"E\"}, {\"b\":\"123\"},{\"s\":\"testCode\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"945\":{\"subfields\":[{\"a\":\"KU/CC/DI/A\"}, {\"b\":\"1234\"}, {\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"945\":{\"subfields\":[{\"h\":\"KU/CC/DI/A\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

  @Test
  public void shouldCreateOneItem() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(PARSED_CONTENT_WITH_MULTIPLE_FIELDS));
    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), new JsonArray().toString());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MappingDetail mappingDetails = new MappingDetail()
      .withName("item")
      .withRecordType(ITEM)
      .withMappingFields(Lists.newArrayList(new MappingRule()
          .withName("barcode")
          .withEnabled("true")
          .withPath("item.barcode")
          .withValue("\"123\"")));

    MappingProfile profile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create testing Items")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(ITEM)
      .withMappingDetails(mappingDetails);

    MappingContext mappingContext = new MappingContext();

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    JsonBasedWriter writer = new JsonBasedWriter(ITEM);
    Mapper mapper = new ItemMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    DataImportEventPayload mappedPayload = mapper.map(profile, eventPayload, mappingContext);
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(ITEM.value()));
    JsonArray items = new JsonArray(mappedPayload.getContext().get(ITEM.value()));
    assertEquals(1, items.size());
    assertEquals("123", items.getJsonObject(0).getJsonObject("item").getString("barcode"));
  }

  @Test
  public void shouldMapExistingItemFromContext() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(PARSED_CONTENT_WITH_MULTIPLE_FIELDS));
    HashMap<String, String> context = new HashMap<>();
    UUID itemId = UUID.randomUUID();
    JsonArray itemsAsJson = new JsonArray(List.of(
      new JsonObject().put("item", new JsonObject().put("id", itemId))));

    context.put(ITEM.value(), itemsAsJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MappingDetail mappingDetails = new MappingDetail()
      .withName("item")
      .withRecordType(ITEM)
      .withMappingFields(Lists.newArrayList(new MappingRule()
        .withName("barcode")
        .withEnabled("true")
        .withPath("item.barcode")
        .withValue("\"123\"")));

    MappingProfile profile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create testing Items")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(ITEM)
      .withMappingDetails(mappingDetails);

    MappingContext mappingContext = new MappingContext();

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    JsonBasedWriter writer = new JsonBasedWriter(ITEM);
    Mapper mapper = new ItemMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    DataImportEventPayload mappedPayload = mapper.map(profile, eventPayload, mappingContext);
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(ITEM.value()));
    JsonArray items = new JsonArray(mappedPayload.getContext().get(ITEM.value()));
    assertEquals(1, items.size());
    assertEquals("123", items.getJsonObject(0).getJsonObject("item").getString("barcode"));
    assertEquals(itemId.toString(), items.getJsonObject(0).getJsonObject("item").getString("id"));
  }

  @Test
  public void shouldCreateMultipleItemPerHoldingsPermanentLocationFields() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(PARSED_CONTENT_WITH_MULTIPLE_FIELDS));
    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), new JsonArray().toString());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    context.put(MULTIPLE_HOLDINGS_FIELD, "945");
    eventPayload.setContext(context);

    MappingDetail mappingDetails = new MappingDetail()
      .withName("item")
      .withRecordType(ITEM)
      .withMappingFields(Lists.newArrayList(new MappingRule()
        .withName("barcode")
        .withEnabled("true")
        .withPath("item.barcode")
        .withValue("945$b"),
        new MappingRule()
          .withName("statisticalCodeIds")
          .withEnabled("true")
          .withPath("item.statisticalCodeIds[]")
          .withValue("")
          .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
          .withSubfields(List.of(
            new RepeatableSubfieldMapping()
              .withOrder(0)
              .withPath("item.statisticalCodeIds[]")
              .withFields(List.of(new MappingRule()
                .withName("statisticalCodeId")
                .withEnabled("true")
                .withPath("item.statisticalCodeIds[]")
                .withValue("\"Testing\""))),
            new RepeatableSubfieldMapping()
              .withOrder(1)
              .withPath("item.statisticalCodeIds[]")
              .withFields(List.of(new MappingRule()
                .withName("statisticalCodeId")
                .withEnabled("true")
                .withPath("item.statisticalCodeIds[]")
                .withValue("945$s; else 944$s")))))));

    MappingProfile profile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create testing Items")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(ITEM)
      .withMappingDetails(mappingDetails);

    MappingContext mappingContext = new MappingContext();

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    JsonBasedWriter writer = new JsonBasedWriter(ITEM);
    Mapper mapper = new ItemMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    DataImportEventPayload mappedPayload = mapper.map(profile, eventPayload, mappingContext);
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(ITEM.value()));
    JsonArray items = new JsonArray(mappedPayload.getContext().get(ITEM.value()));
    assertEquals(3, items.size());
    assertEquals("123", items.getJsonObject(0).getJsonObject("item").getString("barcode"));
    assertEquals("1234", items.getJsonObject(1).getJsonObject("item").getString("barcode"));
    assertNull(items.getJsonObject(2).getJsonObject("item").getString("barcode"));

    assertEquals("Testing", items.getJsonObject(0).getJsonObject("item").getJsonArray("statisticalCodeIds").getString(0));
    assertEquals("testCode", items.getJsonObject(0).getJsonObject("item").getJsonArray("statisticalCodeIds").getString(1));

    assertEquals("Testing", items.getJsonObject(1).getJsonObject("item").getJsonArray("statisticalCodeIds").getString(0));
    assertEquals("testCode2", items.getJsonObject(1).getJsonObject("item").getJsonArray("statisticalCodeIds").getString(1));

    assertEquals("Testing", items.getJsonObject(2).getJsonObject("item").getJsonArray("statisticalCodeIds").getString(0));
    assertEquals("testCode2", items.getJsonObject(2).getJsonObject("item").getJsonArray("statisticalCodeIds").getString(1));
  }

  @Test
  public void shouldNotCreateOneItem() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(PARSED_CONTENT_WITH_MULTIPLE_FIELDS));
    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), new JsonArray().toString());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MappingDetail mappingDetails = null;

    MappingProfile profile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create testing Items")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(ITEM)
      .withMappingDetails(mappingDetails);

    MappingContext mappingContext = new MappingContext();

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    JsonBasedWriter writer = new JsonBasedWriter(ITEM);
    Mapper mapper = new ItemMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    DataImportEventPayload mappedPayload = mapper.map(profile, eventPayload, mappingContext);
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(ITEM.value()));
    JsonArray items = new JsonArray(mappedPayload.getContext().get(ITEM.value()));
    assertEquals(0, items.size());
  }
}
