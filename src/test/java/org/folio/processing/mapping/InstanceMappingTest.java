package org.folio.processing.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.folio.ContributorNameType;
import org.folio.ContributorType;
import org.folio.Identifier;
import org.folio.IdentifierType;
import org.folio.Instance;
import org.folio.InstanceType;
import org.folio.Subject;
import org.folio.SubjectSource;
import org.folio.SubjectType;
import org.folio.processing.TestUtil;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.marc.Record;

@RunWith(JUnit4.class)
public class InstanceMappingTest {

  private final RecordMapper<Instance> mapper = RecordMapperBuilder.buildMapper("MARC_BIB");

  private static final String INSTANCES_PATH = "src/test/resources/org/folio/processing/mapping/instance/instances.json";
  private static final String BIBS_PATH = "src/test/resources/org/folio/processing/mapping/instance/CornellFOLIOExemplars_Bibs.mrc";
  private static final String PRECEDING_FILE_PATH = "src/test/resources/org/folio/processing/mapping/instance/780_785_examples.mrc";
  private static final String BIBS_ERRORS_PATH = "src/test/resources/org/folio/processing/mapping/instance/test1_err.mrc";
  private static final String BIB_WITH_REPEATED_SUBFIELDS_PATH = "src/test/resources/org/folio/processing/mapping/instance/336_repeated_subfields.mrc";
  private static final String BIB_WITH_880_WITH_111_SUBFIELD_VALUE = "src/test/resources/org/folio/processing/mapping/instance/880_111_to_711.mrc";
  private static final String BIB_WITH_880_2_WITH_245_SUBFIELD_VALUE = "src/test/resources/org/folio/processing/mapping/instance/880_245_to_246.mrc";
  private static final String BIB_WITH_880_3_WITH_830_SUBFIELD_VALUE = "src/test/resources/org/folio/processing/mapping/instance/880_to_830.mrc";
  private static final String BIB_WITH_5xx_STAFF_ONLY_INDICATORS = "src/test/resources/org/folio/processing/mapping/instance/5xx_staff_only_indicators.mrc";
  private static final String BIB_WITH_NOT_MAPPED_590_SUBFIELD = "src/test/resources/org/folio/processing/mapping/instance/590_subfield_3.mrc";
  private static final String BIB_WITH_REPEATED_020_SUBFIELDS = "src/test/resources/org/folio/processing/mapping/instance/ISBN.mrc";
  private static final String BIB_WITH_REPEATED_600_SUBFIELDS = "src/test/resources/org/folio/processing/mapping/instance/6xx_subjects.mrc";
  private static final String BIB_WITH_REPEATED_600_SUBFIELD_AND_EMPTY_INDICATOR = "src/test/resources/org/folio/processing/mapping/instance/6xx_subjects_without_indicators.mrc";

  private static final String BIB_WITH_RESOURCE_TYPE_SUBFIELD_VALUE = "src/test/resources/org/folio/processing/mapping/instance/336_subfields_mapping.mrc";
  private static final String BIB_WITH_720_FIELDS = "src/test/resources/org/folio/processing/mapping/instance/720_fields_samples.mrc";
  private static final String BIB_WITH_FIELDS_FOR_ALTERNATIVE_MAPPING = "src/test/resources/org/folio/processing/mapping/instance/fields_for_alternative_mapping_samples.mrc";
  private static final String BIB_WITH_FIELDS_FOR_ALTERNATIVE_MAPPING_WITH_PUNCTUATIONS = "src/test/resources/org/folio/processing/mapping/instance/fields_for_alternative_mapping_samples_with_punctuations.mrc";
  public static final String BIB_WITH_SUBJECT_SOURCES_CODE_IN_2_SUBFIELD = "src/test/resources/org/folio/processing/mapping/instance/subject_source_codes_in_2_subfield.mrc";
  private static final String CLASSIFICATIONS_TEST = "src/test/resources/org/folio/processing/mapping/instance/classificationsTest.mrc";
  private static final String INSTANCES_CLASSIFICATIONS_PATH = "src/test/resources/org/folio/processing/mapping/instance/classificationsTestInstance.json";
  private static final String DEFAULT_MAPPING_RULES_PATH = "src/test/resources/org/folio/processing/mapping/instance/rules.json";
  private static final String DEFAULT_INSTANCE_TYPES_PATH = "src/test/resources/org/folio/processing/mapping/instance/instanceTypes.json";
  private static final String DEFAULT_RESOURCE_IDENTIFIERS_TYPES_PATH = "src/test/resources/org/folio/processing/mapping/instance/resourceIdentifiers.json";
  private static final String DEFAULT_SUBJECT_SOURCES_PATH = "src/test/resources/org/folio/processing/mapping/instance/subjectSources.json";
  private static final String DEFAULT_SUBJECT_TYPES_PATH = "src/test/resources/org/folio/processing/mapping/instance/subjectTypes.json";

  private static final String STUB_FIELD_TYPE_ID = "fe19bae4-da28-472b-be90-d442e2428ead";
  private static final String TXT_INSTANCE_TYPE_ID = "6312d172-f0cf-40f6-b27d-9fa8feaf332f";
  private static final String UNSPECIFIED_INSTANCE_TYPE_ID = "30fffe0e-e985-4144-b2e2-1e8179bdb41f";
  private static final String BIB_WITH_MISSING_URI = "src/test/resources/org/folio/processing/mapping/instance/856_missing_uri.mrc";
  private static final String BIB_WITH_MISSING_SUBFIELD_A = "src/test/resources/org/folio/processing/mapping/instance/100_missing_subfield_a.mrc";
  private static final String BIB_WITH_010Z_SUBFIELD = "src/test/resources/org/folio/processing/mapping/instance/Record_with_010$z.mrc";

  @Test
  public void testMarcToInstance() throws IOException {
    var reader = new MarcStreamReader(
      new ByteArrayInputStream(TestUtil.readFileFromPath(BIBS_PATH).getBytes(StandardCharsets.UTF_8)));
    var expected = new JsonArray(TestUtil.readFileFromPath(INSTANCES_PATH));
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    var actual = new JsonArray();
    try (var factory = Validation.buildDefaultValidatorFactory()) {
      var validator = factory.getValidator();

      while (reader.hasNext()) {
        var os = new ByteArrayOutputStream();
        var writer = new MarcJsonWriter(os);
        writer.write(reader.next());
        var marcJson = new JsonObject(os.toString());
        var actualMappedInstance = mapper.mapRecord(marcJson, new MappingParameters(), mappingRules);
        var violations = validator.validate(actualMappedInstance);
        assertTrue(violations.isEmpty());

        actual.add(JsonObject.mapFrom(actualMappedInstance).put("id", "0"));
      }
    }
    assertEquals(expected.encode(), actual.encode());
  }

  @Test
  public void testMarcToInstanceClassifications() throws IOException {
    var reader = new MarcStreamReader(
      new ByteArrayInputStream(TestUtil.readFileFromPath(CLASSIFICATIONS_TEST).getBytes(StandardCharsets.UTF_8)));
    var expected = new JsonArray(TestUtil.readFileFromPath(INSTANCES_CLASSIFICATIONS_PATH));
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    var actual = new JsonArray();
    try (var factory = Validation.buildDefaultValidatorFactory()) {
      var validator = factory.getValidator();

      while (reader.hasNext()) {
        var os = new ByteArrayOutputStream();
        var writer = new MarcJsonWriter(os);
        writer.write(reader.next());
        var marcJson = new JsonObject(os.toString());
        var actualMappedInstance = mapper.mapRecord(marcJson, new MappingParameters(), mappingRules);
        var violations = validator.validate(actualMappedInstance);
        assertTrue(violations.isEmpty());

        actual.add(JsonObject.mapFrom(actualMappedInstance).put("id", "0"));
      }
    }
    assertEquals(expected.encode(), actual.encode());
  }

  @Test
  public void testMarcToInstanceWithWrongRecords() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIBS_ERRORS_PATH).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));
    int i = 0;
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertNotNull(instance.getInstanceTypeId());
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
      i++;
    }
    assertEquals(50, i);
  }

  @Test
  public void testMarcToInstanceIgnoreSubsequentSubfieldsForInstanceTypeId() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_REPEATED_SUBFIELDS_PATH).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertEquals(STUB_FIELD_TYPE_ID, instance.getInstanceTypeId());
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcToInstance880FieldToContributorMeetingName() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_880_WITH_111_SUBFIELD_VALUE).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertEquals(STUB_FIELD_TYPE_ID, instance.getInstanceTypeId());
      assertNotNull(instance.getContributors().get(1));
      assertEquals("fe19bae4-da28-472b-be90-d442e2428ead", instance.getContributors().get(1).getContributorNameTypeId());
      assertEquals("testingMeetingName", instance.getContributors().get(1).getName());
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcToInstance880FieldToAlternativeTitleName() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_880_2_WITH_245_SUBFIELD_VALUE).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertEquals(STUB_FIELD_TYPE_ID, instance.getInstanceTypeId());
      assertEquals(3, instance.getAlternativeTitles().size());
      assertNotNull(instance.getAlternativeTitles().stream().filter(e -> e.getAlternativeTitle().equals("testingAlternativeTitle")).findAny().orElse(null));
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcToInstance880FieldToSeriesStatement() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_880_3_WITH_830_SUBFIELD_VALUE).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertEquals(STUB_FIELD_TYPE_ID, instance.getInstanceTypeId());
      assertNotNull(instance.getSeries());
      assertEquals(1, instance.getSeries().size());
      assertNotNull(instance.getSeries().stream().filter(e -> e.getValue().equals("testingSeries"))
        .findAny().orElse(null));
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcToInstanceNoteStaffOnlyViaIndicator() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_5xx_STAFF_ONLY_INDICATORS).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertNotNull(instance.getNotes());
      assertEquals(7, instance.getNotes().size());
      assertEquals("Rare copy: Gift of David Pescovitz and Timothy Daly. 12345", instance.getNotes().get(1).getNote());
      assertTrue(instance.getNotes().get(1).getStaffOnly());
      assertEquals("Testing Rare copy: Gift of David Pescovitz and Timothy Daly", instance.getNotes().get(2).getNote());
      assertTrue(instance.getNotes().get(2).getStaffOnly());
      assertEquals("Testing Rare copy 3: Gift of David Pescovitz and Timothy Daly. 123", instance.getNotes().get(3).getNote());
      assertFalse(instance.getNotes().get(3).getStaffOnly());
      assertEquals("Correspondence relating to the collection may be found in Cornell University Libraries. John M. Echols Collection. Records, #13\\\\6\\\\1973", instance.getNotes().get(4).getNote());
      assertFalse(instance.getNotes().get(4).getStaffOnly());
      assertEquals("The note should be marked as stuffOnly", instance.getNotes().get(5).getNote());
      assertTrue(instance.getNotes().get(5).getStaffOnly());
      assertEquals("The note should not be marked as stuffOnly", instance.getNotes().get(6).getNote());
      assertFalse(instance.getNotes().get(6).getStaffOnly());
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcToInstanceRemoveElectronicAccessEntriesWithNoUri() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_MISSING_URI).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters(), mappingRules);
      instance.getElectronicAccess()
        .forEach(electronicAccess ->
          assertNotNull(electronicAccess.getUri()));
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcToInstance100requiredSubfield() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_MISSING_SUBFIELD_A).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters(), mappingRules);
      instance.getContributors()
        .forEach(Assert::assertNull);
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcToInstancePrecedingTitles() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(PRECEDING_FILE_PATH).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    List<JsonObject> array = new ArrayList<>();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters(), mappingRules);
      array.add(JsonObject.mapFrom(instance));
      instance.getSucceedingTitles()
        .forEach(succeedingTitle ->
          {
            assertNotNull(succeedingTitle.getTitle());
            succeedingTitle.getIdentifiers().forEach(id -> {
              assertNotNull(id.getIdentifierTypeId());
              assertNotNull(id.getValue());
            });
          }
        );
      instance.getPrecedingTitles()
        .forEach(precedingTitle ->
          {
            assertNotNull(precedingTitle.getTitle());
            precedingTitle.getIdentifiers().forEach(id -> {
              assertNotNull(id.getIdentifierTypeId());
              assertNotNull(id.getValue());
            });
          }
        );
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcToInstanceNotMappedSubFields() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_NOT_MAPPED_590_SUBFIELD).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertNotNull(instance.getNotes());
      assertEquals(1, instance.getNotes().size());
      assertEquals("Adaptation of Xi xiang ji by Wang Shifu", instance.getNotes().get(0).getNote());
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcToInstanceResourceTypeIdMapping() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_RESOURCE_TYPE_SUBFIELD_VALUE).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));
    String rawInstanceTypes = TestUtil.readFileFromPath(DEFAULT_INSTANCE_TYPES_PATH);
    List<InstanceType> instanceTypes = List.of(new ObjectMapper().readValue(rawInstanceTypes, InstanceType[].class));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    List<Instance> mappedInstances = new ArrayList<>();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters().withInstanceTypes(instanceTypes), mappingRules);
      mappedInstances.add(instance);
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
    assertFalse(mappedInstances.isEmpty());
    assertEquals(4, mappedInstances.size());
    assertEquals(TXT_INSTANCE_TYPE_ID, mappedInstances.get(0).getInstanceTypeId());
    assertEquals(TXT_INSTANCE_TYPE_ID, mappedInstances.get(1).getInstanceTypeId());
    assertEquals(TXT_INSTANCE_TYPE_ID, mappedInstances.get(2).getInstanceTypeId());
    assertEquals(UNSPECIFIED_INSTANCE_TYPE_ID, mappedInstances.get(3).getInstanceTypeId());
  }

  @Test
  public void testMarcToInstanceWithRepeatableISBN() throws IOException {
    final String ISBN_IDENTIFIER_ID = "8261054f-be78-422d-bd51-4ed9f33c3422";
    final String INVALID_ISBN_IDENTIFIER_ID = "fcca2643-406a-482a-b760-7a7f8aec640e";
    final List<Map.Entry<String, String>> expectedResults = List.of(
      Map.entry("9780471622673 (acid-free paper)", ISBN_IDENTIFIER_ID),
      Map.entry("0471725331 (electronic bk.)", ISBN_IDENTIFIER_ID),
      Map.entry("9780471725336 (electronic bk.)", INVALID_ISBN_IDENTIFIER_ID),
      Map.entry("0471725323 (electronic bk.)", INVALID_ISBN_IDENTIFIER_ID),
      Map.entry("9780471725329 (electronic bk.)", ISBN_IDENTIFIER_ID),
      Map.entry("0471622672 (acid-free paper)", INVALID_ISBN_IDENTIFIER_ID));

    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_REPEATED_020_SUBFIELDS).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));
    String rawResourceIdentifierTypes = TestUtil.readFileFromPath(DEFAULT_RESOURCE_IDENTIFIERS_TYPES_PATH);
    List<IdentifierType> instanceTypes = List.of(new ObjectMapper().readValue(rawResourceIdentifierTypes, IdentifierType[].class));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    List<Instance> mappedInstances = new ArrayList<>();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters().withIdentifierTypes(instanceTypes), mappingRules);
      mappedInstances.add(instance);
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
    assertFalse(mappedInstances.isEmpty());
    assertEquals(1, mappedInstances.size());
    List<Identifier> identifierTypes = mappedInstances.get(0).getIdentifiers();
    assertEquals(6, identifierTypes.size());
    IntStream.range(0, expectedResults.size()).forEach(index -> {
      Map.Entry<String, String> expected = expectedResults.get(index);
      Identifier actual = identifierTypes.get(index);
      assertEquals(expected.getValue(), actual.getIdentifierTypeId());
      assertEquals(expected.getKey(), actual.getValue());
    });
  }

  @Test
  public void testMarcToInstanceWithRepeatableSubjects() throws IOException {
    final String FIRST_LIBRARY_SOURCE_ID = "e894d0dc-621d-4b1d-98f6-6f7120eb0d40";
    final String SECOND_LIBRARY_SOURCE_ID = "e894d0dc-621d-4b1d-98f6-6f7120eb0d41";
    final String THIRD_LIBRARY_SOURCE_ID = "e894d0dc-621d-4b1d-98f6-6f7120eb0d42";
    final String FOURTH_LIBRARY_SOURCE_ID = "e894d0dc-621d-4b1d-98f6-6f7120eb0d43";
    final String FIFTH_LIBRARY_SOURCE_ID = "e894d0dc-621d-4b1d-98f6-6f7120eb0d44";
    final String SIXTH_LIBRARY_SOURCE_ID = "e894d0dc-621d-4b1d-98f6-6f7120eb0d45";
    final String SEVENTH_LIBRARY_SOURCE_ID = "e894d0dc-621d-4b1d-98f6-6f7120eb0d46";

    final String FIRST_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b1";
    final String SECOND_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b2";
    final String THIRD_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b3";
    final String FOURTH_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b4";
    final String FIFTH_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b5";
    final String SIXTH_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b6";
    final String SEVENTH_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b7";
    final String EIGHTH_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b8";
    final String NINTH_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff511";

    final List<Subject> expectedResults = List.of(
      new Subject().withValue("Testing 600 subject Testing 600b subject").withSourceId(FIRST_LIBRARY_SOURCE_ID).withTypeId(FIRST_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 600.2 subject").withSourceId(FIFTH_LIBRARY_SOURCE_ID).withTypeId(FIRST_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 610 subject").withSourceId(THIRD_LIBRARY_SOURCE_ID).withTypeId(SECOND_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 611 subject").withSourceId(FOURTH_LIBRARY_SOURCE_ID).withTypeId(THIRD_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 630 subject").withSourceId(FIFTH_LIBRARY_SOURCE_ID).withTypeId(FOURTH_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 647 subject").withSourceId(SIXTH_LIBRARY_SOURCE_ID).withTypeId(FIFTH_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 648 subject").withSourceId(SIXTH_LIBRARY_SOURCE_ID).withTypeId(SIXTH_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 650 subject").withSourceId(SEVENTH_LIBRARY_SOURCE_ID).withTypeId(SEVENTH_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 651 subject").withSourceId(SECOND_LIBRARY_SOURCE_ID).withTypeId(EIGHTH_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 655 subject").withSourceId(SECOND_LIBRARY_SOURCE_ID).withTypeId(NINTH_SUBJECT_TYPE_ID)
    );

    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_REPEATED_600_SUBFIELDS).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));
    String rawSubjectSources = TestUtil.readFileFromPath(DEFAULT_SUBJECT_SOURCES_PATH);
    String rawSubjectTypes = TestUtil.readFileFromPath(DEFAULT_SUBJECT_TYPES_PATH);
    List<SubjectSource> subjectSources = List.of(new ObjectMapper().readValue(rawSubjectSources, SubjectSource[].class));
    List<SubjectType> subjectTypes = List.of(new ObjectMapper().readValue(rawSubjectTypes, SubjectType[].class));


    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    List<Instance> mappedInstances = new ArrayList<>();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters().withSubjectSources(subjectSources).withSubjectTypes(subjectTypes), mappingRules);
      mappedInstances.add(instance);
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
    assertFalse(mappedInstances.isEmpty());
    assertEquals(1, mappedInstances.size());

    Set<Subject> subjects = mappedInstances.get(0).getSubjects();
    assertEquals(10, subjects.size());

    Iterator<Subject> iterator = subjects.iterator();
    expectedResults.forEach(expected -> {
      Subject actual = iterator.next();
      assertEquals(expected.getValue(), actual.getValue());
      assertEquals(expected.getSourceId(), actual.getSourceId());
      assertEquals(expected.getTypeId(), actual.getTypeId());
    });
  }

  @Test
  public void testMarcToInstanceWithRepeatableSubjectsMappedWithTypeButWithoutIndicators() throws IOException {
    final String FIRST_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b1";
    final String SECOND_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b2";
    final String THIRD_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b3";
    final String FOURTH_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b4";
    final String FIFTH_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b5";
    final String SIXTH_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b6";
    final String SEVENTH_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b7";
    final String EIGHTH_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff5b8";
    final String NINTH_SUBJECT_TYPE_ID = "d6488f88-1e74-40ce-81b5-b19a928ff511";

    final List<Subject> expectedResults = List.of(
      new Subject().withValue("Test 600.2 subject").withTypeId(FIRST_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 610 subject").withTypeId(SECOND_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 611 subject").withTypeId(THIRD_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 630 subject").withTypeId(FOURTH_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 647 subject").withTypeId(FIFTH_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 648 subject").withTypeId(SIXTH_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 650 subject").withTypeId(SEVENTH_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 651 subject").withTypeId(EIGHTH_SUBJECT_TYPE_ID),
      new Subject().withValue("Test 655 subject").withTypeId(NINTH_SUBJECT_TYPE_ID)
    );

    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_REPEATED_600_SUBFIELD_AND_EMPTY_INDICATOR).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));
    String rawSubjectSources = TestUtil.readFileFromPath(DEFAULT_SUBJECT_SOURCES_PATH);
    String rawSubjectTypes = TestUtil.readFileFromPath(DEFAULT_SUBJECT_TYPES_PATH);
    List<SubjectSource> subjectSources = List.of(new ObjectMapper().readValue(rawSubjectSources, SubjectSource[].class));
    List<SubjectType> subjectTypes = List.of(new ObjectMapper().readValue(rawSubjectTypes, SubjectType[].class));


    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    List<Instance> mappedInstances = new ArrayList<>();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters().withSubjectSources(subjectSources).withSubjectTypes(subjectTypes), mappingRules);
      mappedInstances.add(instance);
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
    assertFalse(mappedInstances.isEmpty());
    assertEquals(1, mappedInstances.size());

    Set<Subject> subjects = mappedInstances.get(0).getSubjects();
    assertEquals(9, subjects.size());

    Iterator<Subject> iterator = subjects.iterator();
    expectedResults.forEach(expected -> {
      Subject actual = iterator.next();
      assertEquals(expected.getValue(), actual.getValue());
      assertEquals(expected.getSourceId(), actual.getSourceId());
      assertEquals(expected.getTypeId(), actual.getTypeId());
    });
  }

  @Test
  public void testMarcToSubjectSourceIdMappingByCodeFrom2Subfield() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(
      TestUtil.readFileFromPath(BIB_WITH_SUBJECT_SOURCES_CODE_IN_2_SUBFIELD).getBytes(StandardCharsets.UTF_8)));

    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));
    List<SubjectSource> subjectSources = new ObjectMapper()
      .readValue(new File(DEFAULT_SUBJECT_SOURCES_PATH), new TypeReference<>() {});

    String firstSourceId = "e894d0dc-621d-4b1d-98f6-6f7120eb0d40";
    String secondSourceId = "e894d0dc-621d-4b1d-98f6-6f7120eb0d41";
    String thirdSourceId = "e894d0dc-621d-4b1d-98f6-6f7120eb0d42";
    String fourthSourceId = "e894d0dc-621d-4b1d-98f6-6f7120eb0d45";
    String fifthSourceId = "e894d0dc-621d-4b1d-98f6-6f7120eb0d46";

    Map<String, String> subjectValueToSourceId = Map.of(
      "Subject heading 600", firstSourceId,
      "Subject heading 610", secondSourceId,
      "Subject heading 611", thirdSourceId,
      "Subject heading 630", fourthSourceId,
      "Subject heading 647", fifthSourceId,
      "Subject heading 648", firstSourceId,
      "Subject heading 650", secondSourceId,
      "Subject heading 651", thirdSourceId,
      "Subject heading 655", fourthSourceId
    );

    assertTrue(reader.hasNext());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    MarcJsonWriter writer = new MarcJsonWriter(os);
    Record record = reader.next();
    writer.write(record);
    JsonObject marc = new JsonObject(os.toString());
    Instance instance = mapper.mapRecord(marc, new MappingParameters().withSubjectSources(subjectSources), mappingRules);

    assertNotNull(instance.getSubjects());
    assertEquals(9, instance.getSubjects().size());
    instance.getSubjects().forEach(subject -> {
      assertNotNull(subject.getValue());
      assertEquals(subjectValueToSourceId.get(subject.getValue()), subject.getSourceId());
    });
  }

  @Test
  public void testMarc720ToInstanceContributors() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_720_FIELDS)
      .getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    List<ContributorType> contributorTypes = List.of(
      new ContributorType().withName("Author").withCode("aut").withId("1"),
      new ContributorType().withName("Editor").withCode("edi").withId("2"));

    List<ContributorNameType> contributorNameTypes = List.of(
      new ContributorNameType().withName("Personal name").withId("1"),
      new ContributorNameType().withName("Corporate name").withId("2"));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters().withContributorTypes(contributorTypes).withContributorNameTypes(contributorNameTypes), mappingRules);
      assertNotNull(instance.getSource());
      assertEquals(6, instance.getContributors().size());
      // 720 \\$aBoguslawski, Pawel$4aut$4edt should match by first $4 subfield and set contributorTypeId
      assertEquals("Boguslawski, Pawel", instance.getContributors().get(0).getName());
      assertEquals("1", instance.getContributors().get(0).getContributorTypeId());
      assertNull(instance.getContributors().get(0).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(0).getContributorNameTypeId());

      // 720  \\$aCHUJO, T.$eauthor$4edt$4edi should set contributorTypeId by any $4 if it matches
      assertEquals("CHUJO, T.", instance.getContributors().get(1).getName());
      assertEquals("2", instance.getContributors().get(1).getContributorTypeId());
      assertNull(instance.getContributors().get(1).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(1).getContributorNameTypeId());

      // 720 \\$aAbdul Rahman, Alias$eeditor$4edt$4prf should match and set contributorTypeId by $e if all $4 don't match
      assertEquals("Abdul Rahman, Alias", instance.getContributors().get(2).getName());
      assertEquals("2", instance.getContributors().get(2).getContributorTypeId());
      assertNull(instance.getContributors().get(2).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(2).getContributorNameTypeId());

      // 720 \\$aGold, Christopher$eeditor$eauthor should match by $e case insensitively and set contributorTypeId
      assertEquals("Gold, Christopher", instance.getContributors().get(3).getName());
      assertEquals("2", instance.getContributors().get(3).getContributorTypeId());
      assertNull(instance.getContributors().get(3).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(3).getContributorNameTypeId());

      // 720 1\$aKURIHARA, N.$edata contact$ecreator should set data from first $e to the "contributorTypeText" if all $e don't match
      assertEquals("KURIHARA, N.", instance.getContributors().get(4).getName());
      assertNull(instance.getContributors().get(4).getContributorTypeId());
      assertEquals("data contact", instance.getContributors().get(4).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(4).getContributorNameTypeId());

      // 720 2\$aLondon Symphony Orchestra.$eoth$4aut should set "getContributorNameTypeId" as Corporate name if ind1 == 2
      assertEquals("London Symphony Orchestra", instance.getContributors().get(5).getName());
      assertEquals("1", instance.getContributors().get(5).getContributorTypeId());
      assertNull(instance.getContributors().get(5).getContributorTypeText());
      assertEquals("2", instance.getContributors().get(5).getContributorNameTypeId());

      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcAlternativeMappingForInstanceContributors() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_FIELDS_FOR_ALTERNATIVE_MAPPING)
      .getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    List<ContributorType> contributorTypes = List.of(
      new ContributorType().withName("Author").withCode("aut").withId("1"),
      new ContributorType().withName("Editor").withCode("edi").withId("2"));

    List<ContributorNameType> contributorNameTypes = List.of(
      new ContributorNameType().withName("Personal name").withId("1"),
      new ContributorNameType().withName("Corporate name").withId("2"),
      new ContributorNameType().withName("Meeting name").withId("3"));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters().withContributorTypes(contributorTypes).withContributorNameTypes(contributorNameTypes), mappingRules);
      assertNotNull(instance.getSource());
      assertEquals(15, instance.getContributors().size());


      // 100 \1\$aChin, Staceyann,$d1972-$eAuthor$eNarrator$0http://id.loc.gov/authorities/names/n2008052404$1http://viaf.org/viaf/24074052 should match by $e subfield and set contributorTypeId
      assertEquals("Chin, Staceyann, 1972-", instance.getContributors().get(0).getName());
      assertEquals("1", instance.getContributors().get(0).getContributorTypeId());
      assertNull(instance.getContributors().get(0).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(0).getContributorNameTypeId());

      // 110 1\$aOklahoma.$bDept. of Highways.$4cou should match by $e subfield and set contributorTypeId
      assertEquals("Oklahoma. Dept. of Highways", instance.getContributors().get(1).getName());
      assertEquals("1", instance.getContributors().get(1).getContributorTypeId());
      assertNull(instance.getContributors().get(1).getContributorTypeText());
      assertEquals("2", instance.getContributors().get(1).getContributorNameTypeId());

      // 111  2\$aInternational Conference on Business History$4aut
      assertEquals("International Conference on Business History", instance.getContributors().get(2).getName());
      assertEquals("1", instance.getContributors().get(2).getContributorTypeId());
      assertNull(instance.getContributors().get(2).getContributorTypeText());
      assertEquals("3", instance.getContributors().get(2).getContributorNameTypeId());

      // 700 \\$aBoguslawski, Pawel$4aut$4edt should match by first $4 subfield and set contributorTypeId
      assertEquals("Boguslawski, Pawel", instance.getContributors().get(3).getName());
      assertEquals("1", instance.getContributors().get(3).getContributorTypeId());
      assertNull(instance.getContributors().get(3).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(3).getContributorNameTypeId());

      // 700  \\$aCHUJO, T.$eauthor$4edt$4edi should set contributorTypeId by any $4 if it matches
      assertEquals("CHUJO, T.", instance.getContributors().get(4).getName());
      assertEquals("2", instance.getContributors().get(4).getContributorTypeId());
      assertNull(instance.getContributors().get(4).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(4).getContributorNameTypeId());

      // 700 \\$aAbdul Rahman, Alias$eeditor$4edt$4prf should match and set contributorTypeId by $e if all $4 don't match
      assertEquals("Abdul Rahman, Alias", instance.getContributors().get(5).getName());
      assertEquals("2", instance.getContributors().get(5).getContributorTypeId());
      assertNull(instance.getContributors().get(5).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(5).getContributorNameTypeId());

      // 700 \\$aGold, Christopher$eeditor$eauthor should match by $e case insensitively and set contributorTypeId
      assertEquals("Gold, Christopher", instance.getContributors().get(6).getName());
      assertEquals("2", instance.getContributors().get(6).getContributorTypeId());
      assertNull(instance.getContributors().get(6).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(6).getContributorNameTypeId());

      // 700 1\$aKURIHARA, N.$edata contact$jcreator should set data from first $e to the "contributorTypeText" if all $e don't match
      assertEquals("KURIHARA, N.", instance.getContributors().get(7).getName());
      assertNull(instance.getContributors().get(7).getContributorTypeId());
      assertEquals("data contact", instance.getContributors().get(7).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(7).getContributorNameTypeId());

      // 700 2\$aLondon Symphony Orchestra.$eoth$4aut should set "getContributorNameTypeId" as Corporate name if ind1 == 2
      assertEquals("London Symphony Orchestra", instance.getContributors().get(8).getName());
      assertEquals("1", instance.getContributors().get(8).getContributorTypeId());
      assertNull(instance.getContributors().get(8).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(8).getContributorNameTypeId());

      // 711 \\$aBoguslawski, Pawel$4aut$4edt should match by first $4 subfield and set contributorTypeId
      assertEquals("Boguslawski, Pawel", instance.getContributors().get(9).getName());
      assertEquals("1", instance.getContributors().get(9).getContributorTypeId());
      assertNull(instance.getContributors().get(9).getContributorTypeText());
      assertEquals("3", instance.getContributors().get(9).getContributorNameTypeId());

      // 711  \\$aCHUJO, T.$jauthor$4edt$4edi should set contributorTypeId by any $4 if it matches
      assertEquals("CHUJO, T.", instance.getContributors().get(10).getName());
      assertEquals("2", instance.getContributors().get(10).getContributorTypeId());
      assertNull(instance.getContributors().get(10).getContributorTypeText());
      assertEquals("3", instance.getContributors().get(10).getContributorNameTypeId());

      // 711 \\$aAbdul Rahman, Alias$jeditor$4edt$4prf should match and set contributorTypeId by $e if all $4 don't match
      assertEquals("Abdul Rahman, Alias", instance.getContributors().get(11).getName());
      assertEquals("2", instance.getContributors().get(11).getContributorTypeId());
      assertNull(instance.getContributors().get(11).getContributorTypeText());
      assertEquals("3", instance.getContributors().get(11).getContributorNameTypeId());

      // 711 \\$aGold, Christopher$jeditor$jauthor should match by $e case insensitively and set contributorTypeId
      assertEquals("Gold, Christopher", instance.getContributors().get(12).getName());
      assertEquals("2", instance.getContributors().get(12).getContributorTypeId());
      assertNull(instance.getContributors().get(12).getContributorTypeText());
      assertEquals("3", instance.getContributors().get(12).getContributorNameTypeId());

      // 711 1\$aKURIHARA, N.$edata contact$jcreator should set data from first $e to the "contributorTypeText" if all $e don't match
      assertEquals("KURIHARA, N.", instance.getContributors().get(13).getName());
      assertNull(instance.getContributors().get(13).getContributorTypeId());
      assertEquals("data contact", instance.getContributors().get(13).getContributorTypeText());
      assertEquals("3", instance.getContributors().get(13).getContributorNameTypeId());

      // 711 2\$aLondon Symphony Orchestra.$joth$4aut should set "getContributorNameTypeId" as Corporate name if ind1 == 2
      assertEquals("London Symphony Orchestra", instance.getContributors().get(14).getName());
      assertEquals("1", instance.getContributors().get(14).getContributorTypeId());
      assertNull(instance.getContributors().get(14).getContributorTypeText());
      assertEquals("3", instance.getContributors().get(14).getContributorNameTypeId());

      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcAlternativeMappingForInstanceContributorsWithPunctuations() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_FIELDS_FOR_ALTERNATIVE_MAPPING_WITH_PUNCTUATIONS)
      .getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    List<ContributorType> contributorTypes = List.of(
      new ContributorType().withName("Author").withCode("aut").withId("1"),
      new ContributorType().withName("Editor").withCode("edi").withId("2"),
      new ContributorType().withName("Conceptor").withCode("conc").withId("3"),
      new ContributorType().withName("Court reporter").withCode("court").withId("4"),
      new ContributorType().withName("Film distributor").withCode("film").withId("5"),
      new ContributorType().withName("Associated name").withCode("associated").withId("6"),
      new ContributorType().withName("Interviewer").withCode("inter").withId("8"),
      new ContributorType().withName("Author of introduction, etc.").withCode("autofintro").withId("9"),
      new ContributorType().withName("Actor").withCode("act").withId("10")
    );

    List<ContributorNameType> contributorNameTypes = List.of(
      new ContributorNameType().withName("Personal name").withId("1"),
      new ContributorNameType().withName("Corporate name").withId("2"),
      new ContributorNameType().withName("Meeting name").withId("3"));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(os.toString());
      Instance instance = mapper.mapRecord(marc, new MappingParameters().withContributorTypes(contributorTypes).withContributorNameTypes(contributorNameTypes), mappingRules);
      assertNotNull(instance.getSource());
      assertEquals(10, instance.getContributors().size());


      // 100 1\$aKani, John,$econceptor;$ecourt report should match by first $e subfield and set contributorTypeId to 3
      assertEquals("Kani, John", instance.getContributors().get(0).getName());
      assertEquals("3", instance.getContributors().get(0).getContributorTypeId());
      assertNull(instance.getContributors().get(0).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(0).getContributorNameTypeId());

      // 110 2\$aBuena Vista Corporate (Firm),$efilm distributor. should remove comma at the end of the name, match by first $e subfield and set contributorTypeId to 5
      assertEquals("Buena Vista Corporate (Firm)", instance.getContributors().get(1).getName());
      assertEquals("5", instance.getContributors().get(1).getContributorTypeId());
      assertNull(instance.getContributors().get(1).getContributorTypeText());
      assertEquals("2", instance.getContributors().get(1).getContributorNameTypeId());

      // 111 2\$aSuperheroes,$jassociated name;$jdepicted. should remove comma at the end of the name, match by first $j subfield and set contributorTypeId to 6
      assertEquals("Superheroes", instance.getContributors().get(2).getName());
      assertEquals("6", instance.getContributors().get(2).getContributorTypeId());
      assertNull(instance.getContributors().get(2).getContributorTypeText());
      assertEquals("3", instance.getContributors().get(2).getContributorNameTypeId());

      // 700 1\$aBrown, Sterling K.,$eactress;$einterviewer. should remove comma at the end of the name, match by second $e subfield and set contributorTypeId to 8
      assertEquals("Brown, Sterling K.", instance.getContributors().get(3).getName());
      assertEquals("8", instance.getContributors().get(3).getContributorTypeId());
      assertNull(instance.getContributors().get(3).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(3).getContributorNameTypeId());

      // 700 1\$aBrown, Sterling K,.$eactress;$einterviewer. should remove comma at the end of the name, match by second $e subfield and set contributorTypeId to 8
      assertEquals("Brown, Sterling K.", instance.getContributors().get(4).getName());
      assertEquals("8", instance.getContributors().get(4).getContributorTypeId());
      assertNull(instance.getContributors().get(4).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(4).getContributorNameTypeId());

      // 700 1\$aBrown, Sterling K-$$einterviewer. should NOT remove the hyphen at the end of the name, match by second $e subfield and set contributorTypeId to 8
      assertEquals("Brown, Sterling K-", instance.getContributors().get(5).getName());
      assertEquals("8", instance.getContributors().get(5).getContributorTypeId());
      assertNull(instance.getContributors().get(5).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(5).getContributorNameTypeId());

      // 700 1\$aMorrison, Rachel$c(Cinematographer),$edirector of photorgaphy. should remove comma at the end of the name(subfield a+c), not match by $e subfield and set it as contributorTypeText to 8
      assertEquals("Morrison, Rachel (Cinematographer)", instance.getContributors().get(6).getName());
      assertNull(instance.getContributors().get(6).getContributorTypeId());
      assertEquals("director of photorgaphy.", instance.getContributors().get(6).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(6).getContributorNameTypeId());

      // 700 1\$aMorrison, Rachel$c(Cinematographer),$eeAuthor of introduction, etc. should remove comma at the end of the name(subfield a+c), match by $e subfield and set contributorTypeId to 9
      assertEquals("Morrison, Rachel (Cinematographer)", instance.getContributors().get(7).getName());
      assertEquals("9", instance.getContributors().get(7).getContributorTypeId());
      assertNull(instance.getContributors().get(7).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(7).getContributorNameTypeId());

      // 700 1\$aMorrison, Rachel$c(Cinematographer),$eeAuthor of introduction, etc should remove comma at the end of the name(subfield a+c), match by $e subfield and set contributorTypeId to 9
      assertEquals("Morrison, Rachel (Cinematographer)", instance.getContributors().get(8).getName());
      assertEquals("9", instance.getContributors().get(8).getContributorTypeId());
      assertNull(instance.getContributors().get(8).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(8).getContributorNameTypeId());

      // 700 1\$aWright, Letitia,$d1993-$eauthor of introduction, etc.;$eactor. should remove comma at the end of the name(subfield a+c), match by $e author of introduction, etc. subfield and set contributorTypeId to 9
      assertEquals("Wright, Letitia, 1993-", instance.getContributors().get(9).getName());
      assertEquals("9", instance.getContributors().get(9).getContributorTypeId());
      assertNull(instance.getContributors().get(9).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(9).getContributorNameTypeId());

      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcToInstanceForInstanceTypeIds() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_010Z_SUBFIELD).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));
    String rawInstanceTypes = TestUtil.readFileFromPath(DEFAULT_INSTANCE_TYPES_PATH);
    List<InstanceType> instanceTypes = List.of(new ObjectMapper().readValue(rawInstanceTypes, InstanceType[].class));
    String expected010SubfieldZ = "3025698745";
    int expectedSizeOfIdentifiers = 7;

    try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
      List<Instance> mappedInstances = new ArrayList<>();
      while (reader.hasNext()) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        MarcJsonWriter writer = new MarcJsonWriter(os);
        Record record = reader.next();
        writer.write(record);
        JsonObject marc = new JsonObject(os.toString());
        Instance instance = mapper.mapRecord(marc, new MappingParameters().withInstanceTypes(instanceTypes), mappingRules);
        mappedInstances.add(instance);
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
        assertTrue(violations.isEmpty());
      }
      assertFalse(mappedInstances.isEmpty());
      assertEquals(1, mappedInstances.size());
      assertEquals(expectedSizeOfIdentifiers, mappedInstances.get(0).getIdentifiers().size());
      mappedInstances.get(0).getIdentifiers().forEach(Assert::assertNotNull);

      var identifiers = mappedInstances.get(0).getIdentifiers();
      assertTrue(identifiers.stream().map(Identifier::getValue).anyMatch(actualValue -> actualValue.equals(expected010SubfieldZ)));

    }
  }
}
