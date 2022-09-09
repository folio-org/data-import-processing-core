package org.folio.processing.mapping;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import io.vertx.core.json.JsonObject;
import org.folio.AuthoritySourceFile;
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
  private static final String PARSED_AUTHORITY_WITH_SOURCE_FILE_AT_001_AND_010 =
    "src/test/resources/org/folio/processing/mapping/authority/parsedRecordWithSourceFileAt001And010.json";
  private static final String PARSED_AUTHORITY_WITH_SOURCE_FILE_AT_001 =
    "src/test/resources/org/folio/processing/mapping/authority/parsedRecordWithSourceFileAt001.json";
  private static final String PARSED_AUTHORITY_WITH_SOURCE_FILE_AT_010 =
    "src/test/resources/org/folio/processing/mapping/authority/parsedRecordWithSourceFileAt010.json";
  private static final String PARSED_AUTHORITY_WITH_SOURCE_FILE_AT_010_WITH_MULTIPLE_SUBFIELDS =
    "src/test/resources/org/folio/processing/mapping/authority/parsedRecordWithSourceFileAt010WithMultipleSubfields.json";
  private static final String PARSED_AUTHORITY_WITHOUT_SOURCE_FILE =
    "src/test/resources/org/folio/processing/mapping/authority/parsedRecordWithoutSourceFile.json";
  private static final String MAPPED_AUTHORITY_WITH_TITLES_PATH =
    "src/test/resources/org/folio/processing/mapping/authority/mappedRecordWithTitles.json";
  private static final String MAPPED_AUTHORITY_WITHOUT_TITLES_PATH =
    "src/test/resources/org/folio/processing/mapping/authority/mappedRecordWithoutTitles.json";
  private static final String MAPPED_AUTHORITY_WITH_SOURCE_FILE_AT_001_AND_010 =
    "src/test/resources/org/folio/processing/mapping/authority/mappedRecordWithSourceFileAt001And010.json";
  private static final String MAPPED_AUTHORITY_WITH_SOURCE_FILE_AT_001 =
    "src/test/resources/org/folio/processing/mapping/authority/mappedRecordWithSourceFileAt001.json";
  private static final String MAPPED_AUTHORITY_WITH_SOURCE_FILE_AT_010 =
    "src/test/resources/org/folio/processing/mapping/authority/mappedRecordWithSourceFileAt010.json";
  private static final String MAPPED_AUTHORITY_WITH_SOURCE_FILE_AT_010_WITH_MULTIPLE_SUBFIELDS =
    "src/test/resources/org/folio/processing/mapping/authority/mappedRecordWithSourceFileAt010WithMultipleSubfields.json";
  private static final String MAPPED_AUTHORITY_WITHOUT_SOURCE_FILE =
    "src/test/resources/org/folio/processing/mapping/authority/mappedRecordWithoutSourceFile.json";
  private static final String DEFAULT_MAPPING_RULES_PATH =
    "src/test/resources/org/folio/processing/mapping/authority/authorityRules.json";

  private final RecordMapper<Authority> mapper = RecordMapperBuilder.buildMapper("MARC_AUTHORITY");

  private final List<AuthoritySourceFile> authoritySourceFiles = List.of(new AuthoritySourceFile()
      .withId("e2efd148-8c17-41c2-9a4b-3d490db9b158")
      .withName("LC Name Authority file (LCNAF)")
      .withType("Names")
      .withCodes(List.of("n", "nb", "nr", "no")),
    new AuthoritySourceFile()
      .withId("ce023941-e28d-40c1-910b-02e42d6ea2eb")
      .withName("Faceted Application of Subject Terminology (FAST)")
      .withType("Subjects")
      .withCodes(List.of("fst")),
    new AuthoritySourceFile()
      .withId("6ccf7405-6ecf-4c16-9d84-a5bd0774e806")
      .withName("LC Subject Headings (LCSH)")
      .withType("Subjects")
      .withCodes(List.of("sh"))
  );

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

  @Test
  public void testMarcToAuthorityWithSourceFileAt010() throws IOException {
    JsonObject expectedMappedAuthority = new JsonObject(TestUtil.readFileFromPath(MAPPED_AUTHORITY_WITH_SOURCE_FILE_AT_010));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    Authority actualMappedAuthority = mapper
      .mapRecord(getJsonMarcRecord(PARSED_AUTHORITY_WITH_SOURCE_FILE_AT_010),
        new MappingParameters().withAuthoritySourceFiles(authoritySourceFiles), mappingRules);
    Assert.assertEquals(expectedMappedAuthority.encode(), JsonObject.mapFrom(actualMappedAuthority).encode());
  }

  @Test
  public void testMarcToAuthorityWithSourceFileAt001And010() throws IOException {
    JsonObject expectedMappedAuthority = new JsonObject(TestUtil.readFileFromPath(MAPPED_AUTHORITY_WITH_SOURCE_FILE_AT_001_AND_010));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    Authority actualMappedAuthority = mapper
      .mapRecord(getJsonMarcRecord(PARSED_AUTHORITY_WITH_SOURCE_FILE_AT_001_AND_010),
        new MappingParameters().withAuthoritySourceFiles(authoritySourceFiles), mappingRules);
    Assert.assertEquals(expectedMappedAuthority.encode(), JsonObject.mapFrom(actualMappedAuthority).encode());
  }

  @Test
  public void testMarcToAuthorityWithSourceFileAt001() throws IOException {
    JsonObject expectedMappedAuthority = new JsonObject(TestUtil.readFileFromPath(MAPPED_AUTHORITY_WITH_SOURCE_FILE_AT_001));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    Authority actualMappedAuthority = mapper
      .mapRecord(getJsonMarcRecord(PARSED_AUTHORITY_WITH_SOURCE_FILE_AT_001),
        new MappingParameters().withAuthoritySourceFiles(authoritySourceFiles), mappingRules);
    Assert.assertEquals(expectedMappedAuthority.encode(), JsonObject.mapFrom(actualMappedAuthority).encode());
  }

  @Test
  public void testMarcToAuthorityWithSourceFileAt010WithMultipleSubfields() throws IOException {
    JsonObject expectedMappedAuthority = new JsonObject(TestUtil.readFileFromPath(MAPPED_AUTHORITY_WITH_SOURCE_FILE_AT_010_WITH_MULTIPLE_SUBFIELDS));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    Authority actualMappedAuthority = mapper
      .mapRecord(getJsonMarcRecord(PARSED_AUTHORITY_WITH_SOURCE_FILE_AT_010_WITH_MULTIPLE_SUBFIELDS),
        new MappingParameters().withAuthoritySourceFiles(authoritySourceFiles), mappingRules);
    Assert.assertEquals(expectedMappedAuthority.encode(), JsonObject.mapFrom(actualMappedAuthority).encode());
  }

  @Test
  public void testMarcToAuthorityWithoutSourceFile_defaultNaturalId() throws IOException {
    JsonObject expectedMappedAuthority = new JsonObject(TestUtil.readFileFromPath(MAPPED_AUTHORITY_WITHOUT_SOURCE_FILE));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    Authority actualMappedAuthority = mapper
      .mapRecord(getJsonMarcRecord(PARSED_AUTHORITY_WITHOUT_SOURCE_FILE),
        new MappingParameters().withAuthoritySourceFiles(authoritySourceFiles), mappingRules);
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
