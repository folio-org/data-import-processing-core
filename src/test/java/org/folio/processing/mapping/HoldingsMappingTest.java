package org.folio.processing.mapping;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
import org.marc4j.marc.Record;

import org.folio.CallNumberType;
import org.folio.ElectronicAccessRelationship;
import org.folio.Holdings;
import org.folio.HoldingsNoteType;
import org.folio.HoldingsType;
import org.folio.Location;
import org.folio.processing.TestUtil;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

@RunWith(JUnit4.class)
public class HoldingsMappingTest {

  private static final String PARSED_HOLDINGS_PATH =
    "src/test/resources/org/folio/processing/mapping/holdings/parsedHoldingsRecord.json";
  private static final String MAPPED_HOLDINGS_PATH =
    "src/test/resources/org/folio/processing/mapping/holdings/mappedHoldingsRecord.json";
  private static final String DEFAULT_MAPPING_RULES_PATH =
    "src/test/resources/org/folio/processing/mapping/holdings/holdingsRules.json";

  private final RecordMapper<Holdings> mapper = RecordMapperBuilder.buildMapper("MARC_HOLDINGS");

  @Test
  public void testMarcToHoldings() throws IOException {
    JsonObject expectedMappedHoldings = new JsonObject(TestUtil.readFileFromPath(MAPPED_HOLDINGS_PATH));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    Holdings actualMappedHoldings = mapper.mapRecord(getJsonMarcRecord(), getMappingParameters(), mappingRules);
    Assert.assertEquals(expectedMappedHoldings.encode(), JsonObject.mapFrom(actualMappedHoldings).put("id", "0").encode());
  }

  @Test
  public void testMarcToHoldingsWhenHoldingsIdIsUnknown() throws IOException {
    JsonObject expectedMappedHoldings = new JsonObject(TestUtil.readFileFromPath(MAPPED_HOLDINGS_PATH));
    expectedMappedHoldings.remove("holdingsTypeId");
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    var jsonMarcRecord = getJsonMarcRecord();
    jsonMarcRecord.put("leader", "00379cu  a22001334  4500");
    Holdings actualMappedHoldings = mapper.mapRecord(jsonMarcRecord, getMappingParameters(), mappingRules);
    Assert.assertEquals(expectedMappedHoldings.encode(), JsonObject.mapFrom(actualMappedHoldings).put("id", "0").encode());
  }

  private JsonObject getJsonMarcRecord() throws IOException {
    MarcJsonReader reader = new MarcJsonReader(
      new ByteArrayInputStream(TestUtil.readFileFromPath(PARSED_HOLDINGS_PATH).getBytes(StandardCharsets.UTF_8)));
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    MarcJsonWriter writer = new MarcJsonWriter(os);
    Record record = reader.next();
    writer.write(record);
    return new JsonObject(os.toString());
  }

  private MappingParameters getMappingParameters() {
    var mappingParameters = new MappingParameters();
    mappingParameters.setHoldingsTypes(List.of(
      new HoldingsType().withId("00000000-0000-0000-0000-000000000001").withName("Electronic"),
      new HoldingsType().withId("00000000-0000-0000-0000-000000000002").withName("Monograph"),
      new HoldingsType().withId("00000000-0000-0000-0000-000000000003").withName("Multi-part monograph"),
      new HoldingsType().withId("00000000-0000-0000-0000-000000000004").withName("Physical"),
      new HoldingsType().withId("00000000-0000-0000-0000-000000000005").withName("Serial")
    ));
    mappingParameters.setHoldingsNoteTypes(List.of(
      new HoldingsNoteType().withId("00000000-0000-0000-0000-000000000001").withName("Action note"),
      new HoldingsNoteType().withId("00000000-0000-0000-0000-000000000002").withName("Binding"),
      new HoldingsNoteType().withId("00000000-0000-0000-0000-000000000003").withName("Copy note"),
      new HoldingsNoteType().withId("00000000-0000-0000-0000-000000000004").withName("Electronic bookplate"),
      new HoldingsNoteType().withId("00000000-0000-0000-0000-000000000005").withName("Note"),
      new HoldingsNoteType().withId("00000000-0000-0000-0000-000000000006").withName("Provenance"),
      new HoldingsNoteType().withId("00000000-0000-0000-0000-000000000007").withName("Reproduction")
    ));
    mappingParameters.setCallNumberTypes(List.of(
      new CallNumberType().withId("00000000-0000-0000-0000-000000000001").withName("Library of Congress classification"),
      new CallNumberType().withId("00000000-0000-0000-0000-000000000002").withName("Dewey Decimal classification"),
      new CallNumberType().withId("00000000-0000-0000-0000-000000000003")
        .withName("National Library of Medicine classification"),
      new CallNumberType().withId("00000000-0000-0000-0000-000000000004")
        .withName("Superintendent of Documents classification"),
      new CallNumberType().withId("00000000-0000-0000-0000-000000000005").withName("Shelving control number"),
      new CallNumberType().withId("00000000-0000-0000-0000-000000000006").withName("Title"),
      new CallNumberType().withId("00000000-0000-0000-0000-000000000007").withName("Shelved separately"),
      new CallNumberType().withId("00000000-0000-0000-0000-000000000008").withName("Source specified in subfield $2"),
      new CallNumberType().withId("00000000-0000-0000-0000-000000000009").withName("Other scheme")
    ));
    mappingParameters.setElectronicAccessRelationships(List.of(
      new ElectronicAccessRelationship().withId("00000000-0000-0000-0000-000000000001").withName("resource"),
      new ElectronicAccessRelationship().withId("00000000-0000-0000-0000-000000000002").withName("version of resource"),
      new ElectronicAccessRelationship().withId("00000000-0000-0000-0000-000000000003").withName("related resource"),
      new ElectronicAccessRelationship().withId("00000000-0000-0000-0000-000000000004").withName("no information provided")
    ));
    mappingParameters.setLocations(List.of(
      new Location().withId("00000000-0000-0000-0000-000000000001").withName("MUS").withCode("mus")
    ));
    return mappingParameters;
  }

}
