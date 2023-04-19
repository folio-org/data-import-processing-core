package org.folio.processing.mapping.mapper;

import com.google.common.collect.Lists;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.Location;
import org.folio.MappingProfile;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.mappers.HoldingsMapper;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.RepeatableSubfieldMapping;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class HoldingsMapperTest {


  @Test
  public void shouldCreateOneHoldingIfOnlySingleMARCfieldContainsLocation() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"945\":{\"subfields\":[{\"a\":\"OM\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), new JsonObject().toString());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);


    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("fcd64ce1-6995-48f0-840e-89ffa2288371", "Main Library (KU/CC/DI/M)");
    acceptedValues.put("53cf956f-c1df-410b-8bea-27f712cca7c0", "Annex (KU/CC/DI/A)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Online (E)");

    MappingDetail mappingDetails = new MappingDetail()
      .withName("holdings")
      .withRecordType(HOLDINGS)
      .withMappingFields(Lists.newArrayList(new MappingRule()
        .withName("permanentLocationId")
        .withEnabled("true")
        .withPath("holdings.permanentLocationId[]")
        .withValue("")
        .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
        .withSubfields(Lists.newArrayList(new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("holdings.permanentLocationId[]")
          .withFields(Lists.newArrayList(new MappingRule()
            .withName("permanentLocationId")
            .withEnabled("true")
            .withPath("holdings.permanentLocationId[]")
            .withValue("945$h")
            .withAcceptedValues(acceptedValues))))),
        new MappingRule()
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
        .withLocations(Lists.newArrayList(Lists.newArrayList(new Location()
          .withId("fcd64ce1-6995-48f0-840e-89ffa2288371")
          .withName("Main Library")
          .withCode("KU/CC/DI/M")))));


    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    JsonBasedWriter writer = new JsonBasedWriter(EntityType.HOLDINGS);
    Mapper mapper = new HoldingsMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    DataImportEventPayload mappedPayload = mapper.map(profile, eventPayload, mappingContext);
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals( 1, holdings.size());
    JsonObject firstHolding = holdings.getJsonObject(0);
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", firstHolding.getJsonObject("holdings").getString("permanentLocationId"));
    assertEquals("Testing", firstHolding.getJsonObject("holdings").getJsonArray("statisticalCodeIds").getString(0));
    JsonArray holdingsIdentifier = new JsonArray(mappedPayload.getContext().get("HOLDINGS_IDENTIFIERS"));
    assertNotNull(holdingsIdentifier);
    assertEquals(1, holdingsIdentifier.size());
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", holdingsIdentifier.getString(0));
  }

  @Test
  public void shouldCreateSingleHoldingIfLocationsAreTheSame() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"945\":{\"subfields\":[{\"a\":\"OM\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"945\":{\"subfields\":[{\"a\":\"AM\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"945\":{\"subfields\":[{\"a\":\"asdf\"},{\"h\":\"KU/CC/DI/A\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), new JsonObject().toString());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);


    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("fcd64ce1-6995-48f0-840e-89ffa2288371", "Main Library (KU/CC/DI/M)");
    acceptedValues.put("53cf956f-c1df-410b-8bea-27f712cca7c0", "Annex (KU/CC/DI/A)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Online (E)");

    MappingDetail mappingDetails = new MappingDetail()
      .withName("holdings")
      .withRecordType(HOLDINGS)
      .withMappingFields(Lists.newArrayList(new MappingRule()
        .withName("permanentLocationId")
        .withEnabled("true")
        .withPath("holdings.permanentLocationId[]")
        .withValue("")
        .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
        .withSubfields(Lists.newArrayList(new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("holdings.permanentLocationId[]")
          .withFields(Lists.newArrayList(new MappingRule()
            .withName("permanentLocationId")
            .withEnabled("true")
            .withPath("holdings.permanentLocationId[]")
            .withValue("945$h")
            .withAcceptedValues(acceptedValues)))))));

    MappingProfile profile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create testing Holdings")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(HOLDINGS)
      .withMappingDetails(mappingDetails);

    MappingContext mappingContext = new MappingContext()
      .withMappingParameters(new MappingParameters()
        .withLocations(Lists.newArrayList(Lists.newArrayList(new Location()
          .withId("fcd64ce1-6995-48f0-840e-89ffa2288371")
          .withName("Main Library")
          .withCode("KU/CC/DI/M")))));


    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    JsonBasedWriter writer = new JsonBasedWriter(EntityType.HOLDINGS);
    Mapper mapper = new HoldingsMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    DataImportEventPayload mappedPayload = mapper.map(profile, eventPayload, mappingContext);
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals( 2, holdings.size());
    JsonObject secondHoldings = holdings.getJsonObject(0);
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", secondHoldings.getJsonObject("holdings").getString("permanentLocationId"));
    JsonObject firstHoldings = holdings.getJsonObject(1);
    assertEquals("53cf956f-c1df-410b-8bea-27f712cca7c0", firstHoldings.getJsonObject("holdings").getString("permanentLocationId"));
    JsonArray holdingsIdentifier = new JsonArray(mappedPayload.getContext().get("HOLDINGS_IDENTIFIERS"));
    assertNotNull(holdingsIdentifier);
    assertEquals(3, holdingsIdentifier.size());
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", holdingsIdentifier.getString(0));
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", holdingsIdentifier.getString(1));
    assertEquals("53cf956f-c1df-410b-8bea-27f712cca7c0", holdingsIdentifier.getString(2));
  }

  @Test
  public void shouldCreateMultipleHoldingsButWithoutDuplicatedLocations() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"945\":{\"subfields\":[{\"a\":\"OM\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"945\":{\"subfields\":[{\"a\":\"AM\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"945\":{\"subfields\":[{\"a\":\"asdf\"},{\"h\":\"fcd64ce1-6995-48f0-840e-89ffa2288371\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), new JsonObject().toString());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);


    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("fcd64ce1-6995-48f0-840e-89ffa2288371", "Main Library (KU/CC/DI/M)");
    acceptedValues.put("53cf956f-c1df-410b-8bea-27f712cca7c0", "Annex (KU/CC/DI/A)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Online (E)");

    MappingDetail mappingDetails = new MappingDetail()
      .withName("holdings")
      .withRecordType(HOLDINGS)
      .withMappingFields(Lists.newArrayList(new MappingRule()
        .withName("permanentLocationId")
        .withEnabled("true")
        .withPath("holdings.permanentLocationId[]")
        .withValue("")
        .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
        .withSubfields(Lists.newArrayList(new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("holdings.permanentLocationId[]")
          .withFields(Lists.newArrayList(new MappingRule()
            .withName("permanentLocationId")
            .withEnabled("true")
            .withPath("holdings.permanentLocationId[]")
            .withValue("945$h")
            .withAcceptedValues(acceptedValues)))))));

    MappingProfile profile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create testing Holdings")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(HOLDINGS)
      .withMappingDetails(mappingDetails);

    MappingContext mappingContext = new MappingContext()
      .withMappingParameters(new MappingParameters()
        .withLocations(Lists.newArrayList(Lists.newArrayList(new Location()
          .withId("fcd64ce1-6995-48f0-840e-89ffa2288371")
          .withName("Main Library")
          .withCode("KU/CC/DI/M")))));


    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload, mappingContext);

    JsonBasedWriter writer = new JsonBasedWriter(EntityType.HOLDINGS);
    Mapper mapper = new HoldingsMapper(reader, writer);
    mapper.initializeReaderAndWriter(eventPayload, reader, writer, mappingContext);
    DataImportEventPayload mappedPayload = mapper.map(profile, eventPayload, mappingContext);
    assertNotNull(mappedPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(mappedPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(mappedPayload.getContext().get(HOLDINGS.value()));
    assertEquals( 1, holdings.size());
    JsonObject firstHoldings = holdings.getJsonObject(0);
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", firstHoldings.getJsonObject("holdings").getString("permanentLocationId"));
    JsonArray holdingsIdentifier = new JsonArray(mappedPayload.getContext().get("HOLDINGS_IDENTIFIERS"));
    assertNotNull(holdingsIdentifier);
    assertEquals(3, holdingsIdentifier.size());
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", holdingsIdentifier.getString(0));
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", holdingsIdentifier.getString(1));
    assertEquals("fcd64ce1-6995-48f0-840e-89ffa2288371", holdingsIdentifier.getString(2));
  }

  @Test
  public void shouldNotCreateOneHoldingsIfProfileIsInvalid() throws IOException {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    String parsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"945\":{\"subfields\":[{\"a\":\"OM\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(parsedContent));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), new JsonObject().toString());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(record));
    eventPayload.setContext(context);


    HashMap<String, String> acceptedValues = new HashMap<>();
    acceptedValues.put("fcd64ce1-6995-48f0-840e-89ffa2288371", "Main Library (KU/CC/DI/M)");
    acceptedValues.put("53cf956f-c1df-410b-8bea-27f712cca7c0", "Annex (KU/CC/DI/A)");
    acceptedValues.put("184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "Online (E)");

    MappingDetail mappingDetails = null;

    MappingProfile profile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create testing Holdings")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(HOLDINGS)
      .withMappingDetails(mappingDetails);

    MappingContext mappingContext = new MappingContext()
      .withMappingParameters(new MappingParameters()
        .withLocations(Lists.newArrayList(Lists.newArrayList(new Location()
          .withId("fcd64ce1-6995-48f0-840e-89ffa2288371")
          .withName("Main Library")
          .withCode("KU/CC/DI/M")))));


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
}