package org.folio.processing.mapping;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
import org.marc4j.marc.Record;

import org.folio.Authority;
import org.folio.processing.TestUtil;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

@RunWith(JUnit4.class)
public class AuthorityMappingTest {

  private static final String PARSED_AUTHORITY_WITH_TITLES_PATH =
    "src/test/resources/org/folio/processing/mapping/authority/parsedRecordWithTitles.json";
  private static final String PARSED_AUTHORITY_WITHOUT_TITLES_PATH =
    "src/test/resources/org/folio/processing/mapping/authority/parsedRecordWithoutTitles.json";
  private static final String MAPPED_AUTHORITY_WITH_TITLES_PATH =
    "src/test/resources/org/folio/processing/mapping/authority/mappedRecordWithTitles.json";
  private static final String MAPPED_AUTHORITY_WITHOUT_TITLES_PATH =
    "src/test/resources/org/folio/processing/mapping/authority/mappedRecordWithoutTitles.json";
  private static final String DEFAULT_MAPPING_RULES_PATH =
    "src/test/resources/org/folio/processing/mapping/authority/authorityRules.json";

  private final RecordMapper<Authority> mapper = RecordMapperBuilder.buildMapper("MARC_AUTHORITY");

  @Test
  public void testMarcToAuthorityWithTitles() throws IOException {
    JsonObject expectedMappedAuthority = new JsonObject(TestUtil.readFileFromPath(MAPPED_AUTHORITY_WITH_TITLES_PATH));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    Authority actualMappedAuthority = mapper
      .mapRecord(getJsonMarcRecord(PARSED_AUTHORITY_WITH_TITLES_PATH), new MappingParameters(), mappingRules);
    Assert.assertEquals(expectedMappedAuthority.encode(), JsonObject.mapFrom(actualMappedAuthority).encode());
  }

  @Test
  public void testMarcToAuthorityWithoutTitles() throws IOException {
    JsonObject expectedMappedAuthority = new JsonObject(TestUtil.readFileFromPath(MAPPED_AUTHORITY_WITHOUT_TITLES_PATH));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    Authority actualMappedAuthority = mapper
      .mapRecord(getJsonMarcRecord(PARSED_AUTHORITY_WITHOUT_TITLES_PATH), new MappingParameters(), mappingRules);
    Assert.assertEquals(expectedMappedAuthority.encode(), JsonObject.mapFrom(actualMappedAuthority).encode());
  }

  private JsonObject getJsonMarcRecord(String path) throws IOException {
    MarcJsonReader reader = new MarcJsonReader(
      new ByteArrayInputStream(TestUtil.readFileFromPath(path).getBytes(StandardCharsets.UTF_8)));
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    MarcJsonWriter writer = new MarcJsonWriter(os);
    Record record = reader.next();
    writer.write(record);
    return new JsonObject(os.toString());
  }
}
