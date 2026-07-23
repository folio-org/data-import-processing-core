package org.folio.processing.mapping.mapper;

import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.collect.Lists;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.mappers.HoldingsMapper;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Location;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.RepeatableSubfieldMapping;
import org.junit.jupiter.api.Test;

class HoldingsMapperTest {
  private static final String SINGLE_LOCATION_PARSED_CONTENT = """
    {"leader":"01314nam  22003851a 4500","fields":[{"001":"ybp7406411"},{"945":{"subfields":[{"a":"\
    OM"},{"h":"KU/CC/DI/M"}],"ind1":" ","ind2":" "}}]}\
    """;
  private static final String MULTIPLE_LOCATIONS_PARSED_CONTENT = """
    {"leader":"01314nam  22003851a 4500","fields":[{"001":"ybp7406411"},{"945":{"subfields":[{"a":"\
    E"},{"s":"testCode"},{"h":"KU/CC/DI/M"}],"ind1":" ","ind2":" "}},{"945":{"subfields":[{"a":"KU/\
    CC/DI/A"},{"h":"KU/CC/DI/M"}],"ind1":" ","ind2":" "}},{"945":{"subfields":[{"h":"KU/CC/DI/A"}],\
    "ind1":" ","ind2":" "}}]}\
    """;
  private static final String MULTIPLE_LOCATIONS_WITH_944_PARSED_CONTENT = """
    {"leader":"01314nam  22003851a 4500","fields":[{"001":"ybp7406411"},{"944":{"subfields":[{"s":"\
    testCode2"}],"ind1":" ","ind2":" "}}, {"945":{"subfields":[{"a":"E"},{"s":"testCode"},{"h":"KU/\
    CC/DI/M"}],"ind1":" ","ind2":" "}},{"945":{"subfields":[{"a":"KU/CC/DI/A"},{"h":"KU/CC/DI/M"}],\
    "ind1":" ","ind2":" "}},{"945":{"subfields":[{"h":"KU/CC/DI/A"}],"ind1":" ","ind2":" "}}]}\
    """;
  private static final String NO_LOCATION_PARSED_CONTENT = """
    {"leader":"01314nam  22003851a 4500","fields":[{"001":"ybp7406411"}]}\
    """;
  private static final String SAME_LOCATION_PARSED_CONTENT = """
    {"leader":"01314nam  22003851a 4500","fields":[{"001":"ybp7406411"},{"945":{"subfields":[{"a":"\
    OM"},{"h":"KU/CC/DI/M"}],"ind1":" ","ind2":" "}},{"945":{"subfields":[{"a":"AM"},{"h":"KU/CC/DI\
    /M"}],"ind1":" ","ind2":" "}},{"945":{"subfields":[{"a":"asdf"},{"h":"fcd64ce1-6995-48f0-840e-8\
    9ffa2288371"}],"ind1":" ","ind2":" "}}]}\
    """;
  private static final String UPDATE_HOLDINGS_PARSED_CONTENT = """
    {"leader":"01314nam  22003851a 4500","fields":[{"001":"ybp7406411"},{"945":{"subfields":[{"a":"\
    E"},{"s":"testCode"},{"h":"KU/CC/DI/M"}],"ind1":" ","ind2":" "}},{"945":{"subfields":[{"h":"KU/\
    CC/DI/A"}],"ind1":" ","ind2":" "}}]}\
    """;

  @Test
  void shouldCreateOneHoldingIfOnlySingleMarcFieldContainsLocation() throws IOException {
    DataImportEventPayload eventPayload =
      createEventPayload(new JsonArray().toString(), SINGLE_LOCATION_PARSED_CONTENT);
    MappingDetail mappingDetails = createHoldingsDetail(
      permanentLocationRule("945$h"),
      simpleStatisticalCodeRule());
    DataImportEventPayload mappedPayload =
      mapHoldings(eventPayload, createProfile("Create testing Holdings", mappingDetails), createMappingContext());
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals(1, holdings.size());
    JsonObject firstHolding = holdings.getJsonObject(0);
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371",
      firstHolding.getJsonObject("holdings").getString("permanentLocationId"));
    assertEquals("Testing", firstHolding.getJsonObject("holdings").getJsonArray("statisticalCodeIds").getString(0));
    JsonArray holdingsIdentifier = new JsonArray(mappedPayload.getContext().get("HOLDINGS_IDENTIFIERS"));
    assertNotNull(holdingsIdentifier);
    assertEquals(1, holdingsIdentifier.size());
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", holdingsIdentifier.getString(0));
  }

  @Test
  void shouldCreateOneHoldingIfPermanentLocationIsStringValue() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    String parsedContent = SINGLE_LOCATION_PARSED_CONTENT;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), new JsonObject().toString());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MappingDetail mappingDetails = new MappingDetail()
      .withName("holdings")
      .withRecordType(HOLDINGS)
      .withMappingFields(Lists.newArrayList(new MappingRule()
        .withName("permanentLocationId")
        .withEnabled("true")
        .withPath("holdings.permanentLocationId")
        .withValue("\"KU/CC/DI/A\"; else 945$h")));

    MappingProfile profile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create testing Holdings")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(HOLDINGS)
      .withMappingDetails(mappingDetails);

    MappingContext mappingContext = new MappingContext()
      .withMappingParameters(new MappingParameters()
        .withLocations(List.of(
          new Location()
            .withId("fcd64ce1-6995-48f0-840e-89ffa2288371")
            .withName("Main Library")
            .withCode("KU/CC/DI/M"),
          new Location()
            .withId("53cf956f-c1df-410b-8bea-27f712cca7c0")
            .withName("Annex")
            .withCode("KU/CC/DI/A"),
          new Location()
            .withId("184aae84-a5bf-4c6a-85ba-4a7c73026cd5")
            .withName("Online")
            .withCode("E"))));

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    JsonBasedWriter writer = new JsonBasedWriter(EntityType.HOLDINGS);
    Mapper mapper = new HoldingsMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    DataImportEventPayload mappedPayload = mapper.map(profile, eventPayload, mappingContext);
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals(1, holdings.size());
    JsonObject firstHoldings = holdings.getJsonObject(0);
    assertEquals("53cf956f-c1df-410b-8bea-27f712cca7c0",
      firstHoldings.getJsonObject("holdings").getString("permanentLocationId"));
    JsonArray holdingsIdentifier = new JsonArray(mappedPayload.getContext().get("HOLDINGS_IDENTIFIERS"));
    assertNotNull(holdingsIdentifier);
    assertEquals(1, holdingsIdentifier.size());
    assertEquals("53cf956f-c1df-410b-8bea-27f712cca7c0", holdingsIdentifier.getString(0));
  }

  @Test
  void shouldMapMultipleHoldingInExistingHoldings() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    String parsedContent = SINGLE_LOCATION_PARSED_CONTENT;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject().put("holdings", new JsonObject()
        .put("id", UUID.randomUUID())
        .put("permanentLocationId", "184aae84-a5bf-4c6a-85ba-4a7c73026cd5")),
      new JsonObject().put("holdings", new JsonObject()
        .put("id", UUID.randomUUID())
        .put("permanentLocationId", "fcd64ce1-6995-48f0-840e-89ffa2288371"))));

    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), holdingsAsJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MappingDetail mappingDetails = new MappingDetail()
      .withName("holdings")
      .withRecordType(HOLDINGS)
      .withMappingFields(Lists.newArrayList(new MappingRule()
        .withName("statisticalCodeIds")
        .withEnabled("true")
        .withPath("holdings.statisticalCodeIds[]")
        .withValue("\"Testing\"")));

    MappingProfile profile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create testing Holdings")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(HOLDINGS)
      .withMappingDetails(mappingDetails);

    MappingContext mappingContext = new MappingContext()
      .withMappingParameters(new MappingParameters()
        .withLocations(List.of(
          new Location()
            .withId("fcd64ce1-6995-48f0-840e-89ffa2288371")
            .withName("Main Library")
            .withCode("KU/CC/DI/M"),
          new Location()
            .withId("53cf956f-c1df-410b-8bea-27f712cca7c0")
            .withName("Annex")
            .withCode("KU/CC/DI/A"),
          new Location()
            .withId("184aae84-a5bf-4c6a-85ba-4a7c73026cd5")
            .withName("Online")
            .withCode("E"))));

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    JsonBasedWriter writer = new JsonBasedWriter(EntityType.HOLDINGS);
    Mapper mapper = new HoldingsMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    DataImportEventPayload mappedPayload = mapper.map(profile, eventPayload, mappingContext);
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals(2, holdings.size());
    JsonObject firstHoldings = holdings.getJsonObject(0);
    JsonObject secondHoldings = holdings.getJsonObject(1);
    assertEquals("184aae84-a5bf-4c6a-85ba-4a7c73026cd5",
      firstHoldings.getJsonObject("holdings").getString("permanentLocationId"));
    assertEquals("Testing", firstHoldings.getJsonObject("holdings").getJsonArray("statisticalCodeIds").getString(0));
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371",
      secondHoldings.getJsonObject("holdings").getString("permanentLocationId"));
    assertEquals("Testing", secondHoldings.getJsonObject("holdings").getJsonArray("statisticalCodeIds").getString(0));
  }

  @Test
  void shouldMapOneHoldingInExistingHoldingIfPermanentLocationIsStringValue() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    String parsedContent = SINGLE_LOCATION_PARSED_CONTENT;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", UUID.randomUUID())
        .put("permanentLocationId", UUID.randomUUID())));

    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), holdingsAsJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MappingDetail mappingDetails = new MappingDetail()
      .withName("holdings")
      .withRecordType(HOLDINGS)
      .withMappingFields(Lists.newArrayList(new MappingRule()
        .withName("permanentLocationId")
        .withEnabled("true")
        .withPath("holdings.permanentLocationId")
        .withValue("\"KU/CC/DI/A\"; else 945$h")));

    MappingProfile profile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create testing Holdings")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(HOLDINGS)
      .withMappingDetails(mappingDetails);

    MappingContext mappingContext = new MappingContext()
      .withMappingParameters(new MappingParameters()
        .withLocations(List.of(
          new Location()
            .withId("fcd64ce1-6995-48f0-840e-89ffa2288371")
            .withName("Main Library")
            .withCode("KU/CC/DI/M"),
          new Location()
            .withId("53cf956f-c1df-410b-8bea-27f712cca7c0")
            .withName("Annex")
            .withCode("KU/CC/DI/A"),
          new Location()
            .withId("184aae84-a5bf-4c6a-85ba-4a7c73026cd5")
            .withName("Online")
            .withCode("E"))));

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    JsonBasedWriter writer = new JsonBasedWriter(EntityType.HOLDINGS);
    Mapper mapper = new HoldingsMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    DataImportEventPayload mappedPayload = mapper.map(profile, eventPayload, mappingContext);
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals(1, holdings.size());
    JsonObject firstHoldings = holdings.getJsonObject(0);
    assertEquals("53cf956f-c1df-410b-8bea-27f712cca7c0",
      firstHoldings.getJsonObject("holdings").getString("permanentLocationId"));
    JsonArray holdingsIdentifier = new JsonArray(mappedPayload.getContext().get("HOLDINGS_IDENTIFIERS"));
    assertNotNull(holdingsIdentifier);
    assertEquals(1, holdingsIdentifier.size());
    assertEquals("53cf956f-c1df-410b-8bea-27f712cca7c0", holdingsIdentifier.getString(0));
  }

  @Test
  void shouldCreateMultipleHoldingsButWithoutDuplicatedLocations() throws IOException {
    DataImportEventPayload eventPayload =
      createEventPayload(new JsonArray().toString(), MULTIPLE_LOCATIONS_PARSED_CONTENT);
    MappingDetail mappingDetails = createHoldingsDetail(
      permanentLocationRule("945$h"),
      temporaryLocationRule("945$a"),
      repeatableStatisticalCodeRule("945$s"));
    DataImportEventPayload mappedPayload =
      mapHoldings(eventPayload, createProfile("Create testing Holdings", mappingDetails), createMappingContext());
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals(2, holdings.size());
    assertHolding(holdings.getJsonObject(0), "fcd64ce1-6995-48f0-840e-89ffa2288371",
      "184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Testing", "testCode");
    assertHolding(holdings.getJsonObject(1), "53cf956f-c1df-410b-8bea-27f712cca7c0", null, "Testing");
    assertHoldingsIdentifiers(mappedPayload, "fcd64ce1-6995-48f0-840e-89ffa2288371",
      "fcd64ce1-6995-48f0-840e-89ffa2288371", "53cf956f-c1df-410b-8bea-27f712cca7c0");
  }

  @Test
  void shouldCreateMultipleHoldingsButIfLocationMappingRuleContainsElseStatement() throws IOException {
    DataImportEventPayload eventPayload =
      createEventPayload(new JsonArray().toString(), MULTIPLE_LOCATIONS_PARSED_CONTENT);
    MappingDetail mappingDetails = createHoldingsDetail(
      permanentLocationRule("945$h; else \"KU/CC/DI/A\""),
      temporaryLocationRule("945$a"),
      repeatableStatisticalCodeRule("945$s"));
    DataImportEventPayload mappedPayload =
      mapHoldings(eventPayload, createProfile("Create testing Holdings", mappingDetails), createMappingContext());
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals(2, holdings.size());
    assertHolding(holdings.getJsonObject(0), "fcd64ce1-6995-48f0-840e-89ffa2288371",
      "184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Testing", "testCode");
    assertHolding(holdings.getJsonObject(1), "53cf956f-c1df-410b-8bea-27f712cca7c0", null, "Testing");
    assertHoldingsIdentifiers(mappedPayload, "fcd64ce1-6995-48f0-840e-89ffa2288371",
      "fcd64ce1-6995-48f0-840e-89ffa2288371", "53cf956f-c1df-410b-8bea-27f712cca7c0");
  }

  @Test
  void shouldCreateMultipleHoldingsUsingMappingRuleWithElseStatement() throws IOException {
    DataImportEventPayload eventPayload =
      createEventPayload(new JsonArray().toString(), MULTIPLE_LOCATIONS_WITH_944_PARSED_CONTENT);
    MappingDetail mappingDetails = createHoldingsDetail(
      permanentLocationRule("945$h"),
      temporaryLocationRule("945$a"),
      repeatableStatisticalCodeRule("945$s; else 944$s"));
    DataImportEventPayload mappedPayload =
      mapHoldings(eventPayload, createProfile("Create testing Holdings", mappingDetails), createMappingContext());
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals(2, holdings.size());
    assertHolding(holdings.getJsonObject(0), "fcd64ce1-6995-48f0-840e-89ffa2288371",
      "184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Testing", "testCode");
    assertHolding(holdings.getJsonObject(1), "53cf956f-c1df-410b-8bea-27f712cca7c0", null, "Testing", "testCode2");
    assertHoldingsIdentifiers(mappedPayload, "fcd64ce1-6995-48f0-840e-89ffa2288371",
      "fcd64ce1-6995-48f0-840e-89ffa2288371", "53cf956f-c1df-410b-8bea-27f712cca7c0");
  }

  @Test
  void shouldCreateOneHoldingIfNoLocationMarcField() throws IOException {
    DataImportEventPayload eventPayload =
      createEventPayload(new JsonArray().toString(), NO_LOCATION_PARSED_CONTENT);
    MappingDetail mappingDetails = createHoldingsDetail(
      permanentLocationRule("945$h; else \"Online (E)\""),
      temporaryLocationRule("945$a"),
      repeatableStatisticalCodeRule("945$s"));
    DataImportEventPayload mappedPayload =
      mapHoldings(eventPayload, createProfile("Create testing Holdings", mappingDetails), createMappingContext());
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals(1, holdings.size());
    JsonObject firstHoldings = holdings.getJsonObject(0);
    assertEquals("184aae84-a5bf-4c6a-85ba-4a7c73026cd5",
      firstHoldings.getJsonObject("holdings").getString("permanentLocationId"));
    assertNull(firstHoldings.getJsonObject("holdings").getString("temporaryLocationId"));
  }

  @Test
  void shouldCreateSingleHoldingIfLocationsAreTheSame() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    String parsedContent = SAME_LOCATION_PARSED_CONTENT;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), new JsonArray().toString());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MappingDetail mappingDetails = new MappingDetail()
      .withName("holdings")
      .withRecordType(HOLDINGS)
      .withMappingFields(Lists.newArrayList(new MappingRule()
        .withName("permanentLocationId")
        .withEnabled("true")
        .withPath("holdings.permanentLocationId")
        .withValue("945$h")));

    MappingProfile profile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create testing Holdings")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(HOLDINGS)
      .withMappingDetails(mappingDetails);

    MappingContext mappingContext = new MappingContext()
      .withMappingParameters(new MappingParameters()
        .withLocations(List.of(
          new Location()
            .withId("fcd64ce1-6995-48f0-840e-89ffa2288371")
            .withName("Main Library")
            .withCode("KU/CC/DI/M"),
          new Location()
            .withId("53cf956f-c1df-410b-8bea-27f712cca7c0")
            .withName("Annex")
            .withCode("KU/CC/DI/A"),
          new Location()
            .withId("184aae84-a5bf-4c6a-85ba-4a7c73026cd5")
            .withName("Online")
            .withCode("E"))));

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    JsonBasedWriter writer = new JsonBasedWriter(EntityType.HOLDINGS);
    Mapper mapper = new HoldingsMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    DataImportEventPayload mappedPayload = mapper.map(profile, eventPayload, mappingContext);
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals(1, holdings.size());
    JsonObject firstHoldings = holdings.getJsonObject(0);
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371",
      firstHoldings.getJsonObject("holdings").getString("permanentLocationId"));
    JsonArray holdingsIdentifier = new JsonArray(mappedPayload.getContext().get("HOLDINGS_IDENTIFIERS"));
    assertNotNull(holdingsIdentifier);
    assertEquals(3, holdingsIdentifier.size());
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", holdingsIdentifier.getString(0));
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", holdingsIdentifier.getString(1));
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", holdingsIdentifier.getString(2));
  }

  @Test
  void shouldNotCreateOneHoldingsIfProfileIsInvalid() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    String parsedContent = SINGLE_LOCATION_PARSED_CONTENT;
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), new JsonObject().toString());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);

    MappingDetail mappingDetails = null;

    MappingProfile profile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create testing Holdings")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(HOLDINGS)
      .withMappingDetails(mappingDetails);

    MappingContext mappingContext = new MappingContext()
      .withMappingParameters(new MappingParameters()
        .withLocations(List.of(
          new Location()
            .withId("fcd64ce1-6995-48f0-840e-89ffa2288371")
            .withName("Main Library")
            .withCode("KU/CC/DI/M"),
          new Location()
            .withId("53cf956f-c1df-410b-8bea-27f712cca7c0")
            .withName("Annex")
            .withCode("KU/CC/DI/A"),
          new Location()
            .withId("184aae84-a5bf-4c6a-85ba-4a7c73026cd5")
            .withName("Online")
            .withCode("E"))));

    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    JsonBasedWriter writer = new JsonBasedWriter(EntityType.HOLDINGS);
    Mapper mapper = new HoldingsMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    DataImportEventPayload mappedPayload = mapper.map(profile, eventPayload, mappingContext);
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonObject holdings = new JsonObject(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals(0, holdings.size());
    assertNull(mappedPayload.getContext().get("HOLDINGS_IDENTIFIERS"));
  }

  @Test
  void shouldUpdateSingleHoldingsButWithoutDuplicatedLocations() throws IOException {
    JsonArray existedHoldings = new JsonArray()
      .add(existingHolding(
        String.valueOf(UUID.randomUUID()),
        String.valueOf(UUID.randomUUID()),
        String.valueOf(UUID.randomUUID()),
        String.valueOf(UUID.randomUUID())))
      .add(existingHolding(
        String.valueOf(UUID.randomUUID()),
        String.valueOf(UUID.randomUUID()),
        String.valueOf(UUID.randomUUID()),
        String.valueOf(UUID.randomUUID())));
    DataImportEventPayload eventPayload =
      createEventPayload(existedHoldings.encode(), UPDATE_HOLDINGS_PARSED_CONTENT);
    MappingDetail mappingDetails = createHoldingsDetail(
      permanentLocationRule("945$h"),
      temporaryLocationRule("945$a"),
      repeatableStatisticalCodeRule("945$s"));
    DataImportEventPayload mappedPayload =
      mapHoldings(eventPayload, createProfile("Update testing Holdings", mappingDetails), createMappingContext());
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertHolding(holdings.getJsonObject(0), "fcd64ce1-6995-48f0-840e-89ffa2288371",
      "184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Testing", "testCode");
    assertHoldingsIdentifiers(mappedPayload, "fcd64ce1-6995-48f0-840e-89ffa2288371",
      "fcd64ce1-6995-48f0-840e-89ffa2288371");
  }

  private DataImportEventPayload createEventPayload(String holdingsValue, String parsedContent) {
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedContent));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), holdingsValue);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(context);
    return eventPayload;
  }

  private JsonObject existingHolding(String id, String hrid, String instanceId, String permanentLocationId) {
    return new JsonObject().put("holdings", new JsonObject()
      .put("id", id)
      .put("hrid", hrid)
      .put("instanceId", instanceId)
      .put("permanentLocationId", permanentLocationId));
  }

  private MappingDetail createHoldingsDetail(MappingRule... mappingRules) {
    return new MappingDetail()
      .withName("holdings")
      .withRecordType(HOLDINGS)
      .withMappingFields(Lists.newArrayList(mappingRules));
  }

  private MappingProfile createProfile(String name, MappingDetail mappingDetails) {
    return new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName(name)
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(HOLDINGS)
      .withMappingDetails(mappingDetails);
  }

  private MappingContext createMappingContext() {
    return new MappingContext()
      .withMappingParameters(new MappingParameters()
        .withLocations(List.of(
          new Location().withId("fcd64ce1-6995-48f0-840e-89ffa2288371").withName("Main Library").withCode("KU/CC/DI/M"),
          new Location().withId("53cf956f-c1df-410b-8bea-27f712cca7c0").withName("Annex").withCode("KU/CC/DI/A"),
          new Location().withId("184aae84-a5bf-4c6a-85ba-4a7c73026cd5").withName("Online").withCode("E"))));
  }

  private DataImportEventPayload mapHoldings(
    DataImportEventPayload eventPayload,
    MappingProfile profile,
    MappingContext mappingContext
  ) throws IOException {
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);
    JsonBasedWriter writer = new JsonBasedWriter(EntityType.HOLDINGS);
    Mapper mapper = new HoldingsMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    return mapper.map(profile, eventPayload, mappingContext);
  }

  private MappingRule permanentLocationRule(String value) {
    return new MappingRule()
      .withName("permanentLocationId")
      .withEnabled("true")
      .withPath("holdings.permanentLocationId")
      .withValue(value);
  }

  private MappingRule temporaryLocationRule(String value) {
    return new MappingRule()
      .withName("temporaryLocationId")
      .withEnabled("true")
      .withPath("holdings.temporaryLocationId")
      .withValue(value);
  }

  private MappingRule simpleStatisticalCodeRule() {
    return new MappingRule()
      .withName("statisticalCodeIds")
      .withEnabled("true")
      .withPath("holdings.statisticalCodeIds[]")
      .withValue("\"Testing\"");
  }

  private MappingRule repeatableStatisticalCodeRule(String value) {
    return new MappingRule()
      .withName("statisticalCodeIds")
      .withEnabled("true")
      .withPath("holdings.statisticalCodeIds[]")
      .withValue("")
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(List.of(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("holdings.statisticalCodeIds[]")
          .withFields(List.of(new MappingRule()
            .withName("statisticalCodeId")
            .withEnabled("true")
            .withPath("holdings.statisticalCodeIds[]")
            .withValue("\"Testing\""))),
        new RepeatableSubfieldMapping()
          .withOrder(1)
          .withPath("holdings.statisticalCodeIds[]")
          .withFields(List.of(new MappingRule()
            .withName("statisticalCodeId")
            .withEnabled("true")
            .withPath("holdings.statisticalCodeIds[]")
            .withValue(value)))));
  }

  private void assertHolding(
    JsonObject holding,
    String permanentLocationId,
    String temporaryLocationId,
    String... statisticalCodeIds
  ) {
    assertEquals(permanentLocationId, holding.getJsonObject("holdings").getString("permanentLocationId"));
    assertEquals(temporaryLocationId, holding.getJsonObject("holdings").getString("temporaryLocationId"));
    JsonArray statisticalCodes = holding.getJsonObject("holdings").getJsonArray("statisticalCodeIds");
    assertEquals(statisticalCodeIds.length, statisticalCodes.size());
    for (int i = 0; i < statisticalCodeIds.length; i++) {
      assertEquals(statisticalCodeIds[i], statisticalCodes.getString(i));
    }
  }

  private void assertHoldingsIdentifiers(DataImportEventPayload mappedPayload, String... expectedIdentifiers) {
    JsonArray holdingsIdentifier = new JsonArray(mappedPayload.getContext().get("HOLDINGS_IDENTIFIERS"));
    assertNotNull(holdingsIdentifier);
    assertEquals(expectedIdentifiers.length, holdingsIdentifier.size());
    for (int i = 0; i < expectedIdentifiers.length; i++) {
      assertEquals(expectedIdentifiers[i], holdingsIdentifier.getString(i));
    }
  }
}
