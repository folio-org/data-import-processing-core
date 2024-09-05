package org.folio.processing.mapping;


import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.folio.Authority;
import org.folio.processing.TestUtil;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
import org.marc4j.marc.Record;

@RunWith(JUnit4.class)
public class AuthorityExtendedMappingTest {

  private static final String MAPPED_AUTHORITY_RECORD_EXTENDED =
    "src/test/resources/org/folio/processing/mapping/authority/mappedRecordExtended.json";
  private static final String DEFAULT_MAPPING_RULES_PATH =
    "src/test/resources/org/folio/processing/mapping/authority/authorityRules.json";
  private static final String PARSED_AUTHORITY_RECORD_EXTENDED =
    "src/test/resources/org/folio/processing/mapping/authority/parsedRecordExtended.json";

  private final RecordMapper<Authority> mapper = RecordMapperBuilder.buildMapper("MARC_AUTHORITY_EXTENDED");

  @Test
  public void testMarcToAuthorityExtendedMapping() throws IOException {
    JsonObject expectedMappedAuthority = new JsonObject(TestUtil.readFileFromPath(MAPPED_AUTHORITY_RECORD_EXTENDED));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    Authority actualMappedAuthority = mapper
      .mapRecord(getJsonMarcRecord(), new MappingParameters(), mappingRules);
    Assert.assertEquals(expectedMappedAuthority.encode(), JsonObject.mapFrom(actualMappedAuthority).encode());
  }

  private JsonObject getJsonMarcRecord() throws IOException {
    MarcJsonReader reader = new MarcJsonReader(
      new ByteArrayInputStream(
        TestUtil.readFileFromPath(PARSED_AUTHORITY_RECORD_EXTENDED).getBytes(StandardCharsets.UTF_8)));
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    MarcJsonWriter writer = new MarcJsonWriter(os);
    Record record = reader.next();
    writer.write(record);
    return new JsonObject(os.toString());
  }
}
