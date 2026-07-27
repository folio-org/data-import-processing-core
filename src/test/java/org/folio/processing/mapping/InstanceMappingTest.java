package org.folio.processing.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
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
import java.util.UUID;
import java.util.stream.IntStream;
import org.folio.Contributor;
import org.folio.Identifier;
import org.folio.Instance;
import org.folio.Subject;
import org.folio.processing.TestUtil;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ContributorNameType;
import org.folio.rest.jaxrs.model.ContributorType;
import org.folio.rest.jaxrs.model.IdentifierType;
import org.folio.rest.jaxrs.model.InstanceDateType;
import org.folio.rest.jaxrs.model.InstanceFormat;
import org.folio.rest.jaxrs.model.InstanceType;
import org.folio.rest.jaxrs.model.IssuanceMode;
import org.folio.rest.jaxrs.model.SubjectSource;
import org.folio.rest.jaxrs.model.SubjectType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.marc.Record;

class InstanceMappingTest {

  static final String BIB_WITH_SUBJECT_SOURCES_CODE_IN_2_SUBFIELD =
    "src/test/resources/org/folio/processing/mapping/instance/subject_source_codes_in_2_subfield.mrc";
  private static final String INSTANCES_PATH =
    "src/test/resources/org/folio/processing/mapping/instance/instances.json";
  private static final String BIBS_PATH =
    "src/test/resources/org/folio/processing/mapping/instance/CornellFOLIOExemplars_Bibs.mrc";
  private static final String PRECEDING_FILE_PATH =
    "src/test/resources/org/folio/processing/mapping/instance/780_785_examples.mrc";
  private static final String BIBS_ERRORS_PATH =
    "src/test/resources/org/folio/processing/mapping/instance/test1_err.mrc";
  private static final String BIB_WITH_REPEATED_SUBFIELDS_PATH =
    "src/test/resources/org/folio/processing/mapping/instance/336_repeated_subfields.mrc";
  private static final String BIB_WITH_880_WITH_111_SUBFIELD_VALUE =
    "src/test/resources/org/folio/processing/mapping/instance/880_111_to_711.mrc";
  private static final String BIB_WITH_880_2_WITH_245_SUBFIELD_VALUE =
    "src/test/resources/org/folio/processing/mapping/instance/880_245_to_246.mrc";
  private static final String BIB_WITH_880_3_WITH_830_SUBFIELD_VALUE =
    "src/test/resources/org/folio/processing/mapping/instance/880_to_830.mrc";
  private static final String BIB_WITH_5XX_STAFF_ONLY_INDICATORS =
    "src/test/resources/org/folio/processing/mapping/instance/5xx_staff_only_indicators.mrc";
  private static final String BIB_WITH_NOT_MAPPED_590_SUBFIELD =
    "src/test/resources/org/folio/processing/mapping/instance/590_subfield_3.mrc";
  private static final String BIB_WITH_REPEATED_020_SUBFIELDS =
    "src/test/resources/org/folio/processing/mapping/instance/ISBN.mrc";
  private static final String BIB_WITH_REPEATED_600_SUBFIELDS =
    "src/test/resources/org/folio/processing/mapping/instance/6xx_subjects.mrc";
  private static final String BIB_WITH_REPEATED_600_SUBFIELD_AND_EMPTY_INDICATOR =
    "src/test/resources/org/folio/processing/mapping/instance/6xx_subjects_without_indicators.mrc";
  private static final String BIB_WITH_008_DATE =
    "src/test/resources/org/folio/processing/mapping/instance/008_date.mrc";
  private static final String BIB_WITHOUT_008_DATE =
    "src/test/resources/org/folio/processing/mapping/instance/008_empty_date.mrc";
  private static final String BIB_WITH_INVALID_008_FIELD =
    "src/test/resources/org/folio/processing/mapping/instance/008_invalid_field.mrc";
  private static final String BIB_WITH_DELETED_LEADER =
    "src/test/resources/org/folio/processing/mapping/instance/deleted_leader.mrc";
  private static final String BIB_WITH_RESOURCE_TYPE_SUBFIELD_VALUE =
    "src/test/resources/org/folio/processing/mapping/instance/336_subfields_mapping.mrc";
  private static final String BIB_WITH_720_FIELDS =
    "src/test/resources/org/folio/processing/mapping/instance/720_fields_samples.mrc";
  private static final String BIB_WITH_FIELDS_FOR_ALTERNATIVE_MAPPING =
    "src/test/resources/org/folio/processing/mapping/instance/fields_for_alternative_mapping_samples.mrc";
  private static final String BIB_WITH_FIELDS_FOR_ALTERNATIVE_MAPPING_WITH_PUNCTUATIONS =
    "src/test/resources/org/folio/processing/mapping/instance/"
    + "fields_for_alternative_mapping_samples_with_punctuations.mrc";
  private static final String CLASSIFICATIONS_TEST =
    "src/test/resources/org/folio/processing/mapping/instance/classificationsTest.mrc";
  private static final String INSTANCES_CLASSIFICATIONS_PATH =
    "src/test/resources/org/folio/processing/mapping/instance/classificationsTestInstance.json";
  private static final String DEFAULT_MAPPING_RULES_PATH =
    "src/test/resources/org/folio/processing/mapping/instance/rules.json";
  private static final String DEFAULT_INSTANCE_TYPES_PATH =
    "src/test/resources/org/folio/processing/mapping/instance/instanceTypes.json";
  private static final String DEFAULT_RESOURCE_IDENTIFIERS_TYPES_PATH =
    "src/test/resources/org/folio/processing/mapping/instance/resourceIdentifiers.json";
  private static final String DEFAULT_SUBJECT_SOURCES_PATH =
    "src/test/resources/org/folio/processing/mapping/instance/subjectSources.json";
  private static final String DEFAULT_SUBJECT_TYPES_PATH =
    "src/test/resources/org/folio/processing/mapping/instance/subjectTypes.json";
  private static final String DEFAULT_INSTANCE_DATE_TYPES_PATH =
    "src/test/resources/org/folio/processing/mapping/instance/instanceDateTypes.json";
  private static final String BIB_WITH_FORMAT_SUBFIELD_VALUE =
    "src/test/resources/org/folio/processing/mapping/instance/338_subfields_mapping.mrc";
  private static final String DEFAULT_INSTANCE_FORMAT_IDENTIFIERS =
    "src/test/resources/org/folio/processing/mapping/instance/formatIdentifiers.json";
  private static final String STUB_FIELD_TYPE_ID = "fe19bae4-da28-472b-be90-d442e2428ead";
  private static final String TXT_INSTANCE_TYPE_ID = "6312d172-f0cf-40f6-b27d-9fa8feaf332f";
  private static final String UNSPECIFIED_INSTANCE_TYPE_ID = "30fffe0e-e985-4144-b2e2-1e8179bdb41f";
  private static final String BIB_WITH_MISSING_URI =
    "src/test/resources/org/folio/processing/mapping/instance/856_missing_uri.mrc";
  private static final String BIB_WITH_MISSING_SUBFIELD_A =
    "src/test/resources/org/folio/processing/mapping/instance/100_missing_subfield_a.mrc";
  private static final String BIB_WITH_010Z_SUBFIELD =
    "src/test/resources/org/folio/processing/mapping/instance/Record_with_010$z.mrc";
  private static final String BIB_WITH_MISSING_001 =
    "src/test/resources/org/folio/processing/mapping/instance/recordWithout001Field.mrc";
  private final RecordMapper<Instance> mapper = RecordMapperBuilder.buildMapper("MARC_BIB");

  @Test
  void testMarcToInstance() throws IOException {
    var reader = newMarcReader(BIBS_PATH);
    var expected = new JsonArray(TestUtil.readFileFromPath(INSTANCES_PATH));
    var mappingRules = loadMappingRules();
    var validator = getValidator();

    var actual = new JsonArray();
    while (reader.hasNext()) {
      var parsedRecord = marcRecordToJson(reader.next());
      var actualMappedInstance = mapper.mapRecord(parsedRecord, new MappingParameters(), mappingRules);
      assertTrue(validator.validate(actualMappedInstance).isEmpty());

      actual.add(JsonObject.mapFrom(actualMappedInstance).put("id", "0"));
    }
    assertEquals(expected.encode(), actual.encode());
  }

  @Test
  void testMarcToInstanceClassifications() throws IOException {
    var reader = newMarcReader(CLASSIFICATIONS_TEST);
    var expected = new JsonArray(TestUtil.readFileFromPath(INSTANCES_CLASSIFICATIONS_PATH));
    var mappingRules = loadMappingRules();
    var validator = getValidator();

    var actual = new JsonArray();
    while (reader.hasNext()) {
      var parsedRecord = marcRecordToJson(reader.next());
      var actualMappedInstance = mapper.mapRecord(parsedRecord, new MappingParameters(), mappingRules);
      assertTrue(validator.validate(actualMappedInstance).isEmpty());

      actual.add(JsonObject.mapFrom(actualMappedInstance).put("id", "0"));
    }
    assertEquals(expected.encode(), actual.encode());
  }

  @Test
  void testMarcToInstanceWithWrongRecords() throws IOException {
    var reader = newMarcReader(BIBS_ERRORS_PATH);
    var mappingRules = loadMappingRules();
    var validator = getValidator();

    int i = 0;
    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertNotNull(instance.getInstanceTypeId());
      assertTrue(validator.validate(instance).isEmpty());
      i++;
    }
    assertEquals(50, i);
  }

  @Test
  void testMarcToInstanceIgnoreSubsequentSubfieldsForInstanceTypeId() throws IOException {
    var reader = newMarcReader(BIB_WITH_REPEATED_SUBFIELDS_PATH);
    var mappingRules = loadMappingRules();
    var validator = getValidator();

    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertEquals(STUB_FIELD_TYPE_ID, instance.getInstanceTypeId());
      assertTrue(validator.validate(instance).isEmpty());
    }
  }

  @Test
  void testMarcToInstanceLeaderToModeIssuance() throws IOException {
    var reader = newMarcReader(BIB_WITH_MISSING_001);
    var mappingRules = loadMappingRules();
    IssuanceMode issuanceMode = new IssuanceMode().withId(UUID.randomUUID().toString())
      .withName("unspecified").withSource("rdamodeissue");
    var mappingParameters = new MappingParameters().withIssuanceModes(List.of(issuanceMode));
    var validator = getValidator();

    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), mappingParameters, mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getModeOfIssuanceId());
      assertNotNull(instance.getSource());
      assertTrue(validator.validate(instance).isEmpty());
    }
  }

  @Test
  void testMarcToInstance880FieldToContributorMeetingName() throws IOException {
    var reader = newMarcReader(BIB_WITH_880_WITH_111_SUBFIELD_VALUE);
    var mappingRules = loadMappingRules();
    var validator = getValidator();

    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertEquals(STUB_FIELD_TYPE_ID, instance.getInstanceTypeId());
      assertNotNull(instance.getContributors().get(1));
      assertEquals("fe19bae4-da28-472b-be90-d442e2428ead",
        instance.getContributors().get(1).getContributorNameTypeId());
      assertEquals("testingMeetingName", instance.getContributors().get(1).getName());
      assertTrue(validator.validate(instance).isEmpty());
    }
  }

  @Test
  void testMarcToInstance880FieldToAlternativeTitleName() throws IOException {
    var reader = newMarcReader(BIB_WITH_880_2_WITH_245_SUBFIELD_VALUE);
    var mappingRules = loadMappingRules();
    var validator = getValidator();

    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertEquals(STUB_FIELD_TYPE_ID, instance.getInstanceTypeId());
      assertEquals(3, instance.getAlternativeTitles().size());
      assertNotNull(instance.getAlternativeTitles().stream()
        .filter(e -> e.getAlternativeTitle().equals("testingAlternativeTitle"))
        .findAny().orElse(null));
      assertTrue(validator.validate(instance).isEmpty());
    }
  }

  @Test
  void testMarcToInstance880FieldToSeriesStatement() throws IOException {
    var reader = newMarcReader(BIB_WITH_880_3_WITH_830_SUBFIELD_VALUE);
    var mappingRules = loadMappingRules();
    var validator = getValidator();

    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertEquals(STUB_FIELD_TYPE_ID, instance.getInstanceTypeId());
      assertNotNull(instance.getSeries());
      assertEquals(1, instance.getSeries().size());
      assertNotNull(instance.getSeries().stream()
        .filter(e -> e.getValue().equals("testingSeries"))
        .findAny().orElse(null));
      assertTrue(validator.validate(instance).isEmpty());
    }
  }

  @Test
  void testMarcToInstanceNoteStaffOnlyViaIndicator() throws IOException {
    var reader = newMarcReader(BIB_WITH_5XX_STAFF_ONLY_INDICATORS);
    var mappingRules = loadMappingRules();
    var validator = getValidator();

    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertNotNull(instance.getNotes());
      assertEquals(7, instance.getNotes().size());
      assertEquals("Rare copy: Gift of David Pescovitz and Timothy Daly. 12345", instance.getNotes().get(1).getNote());
      assertTrue(instance.getNotes().get(1).getStaffOnly());
      assertEquals("Testing Rare copy: Gift of David Pescovitz and Timothy Daly", instance.getNotes().get(2).getNote());
      assertTrue(instance.getNotes().get(2).getStaffOnly());
      assertEquals("Testing Rare copy 3: Gift of David Pescovitz and Timothy Daly. 123",
        instance.getNotes().get(3).getNote());
      assertFalse(instance.getNotes().get(3).getStaffOnly());
      assertEquals(
        "Correspondence relating to the collection may be found in Cornell University Libraries. "
        + "John M. Echols Collection. Records, #13\\6\\1973",
        instance.getNotes().get(4).getNote());
      assertFalse(instance.getNotes().get(4).getStaffOnly());
      assertEquals("The note should be marked as stuffOnly", instance.getNotes().get(5).getNote());
      assertTrue(instance.getNotes().get(5).getStaffOnly());
      assertEquals("The note should not be marked as stuffOnly", instance.getNotes().get(6).getNote());
      assertFalse(instance.getNotes().get(6).getStaffOnly());
      assertTrue(validator.validate(instance).isEmpty());
    }
  }

  @Test
  void testMarcToInstanceRemoveElectronicAccessEntriesWithNoUri() throws IOException {
    var reader = newMarcReader(BIB_WITH_MISSING_URI);
    var mappingRules = loadMappingRules();
    var validator = getValidator();

    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), new MappingParameters(), mappingRules);
      instance.getElectronicAccess()
        .forEach(electronicAccess ->
          assertNotNull(electronicAccess.getUri()));
      assertTrue(validator.validate(instance).isEmpty());
    }
  }

  @Test
  void testMarcToInstance100requiredSubfield() throws IOException {
    var reader = newMarcReader(BIB_WITH_MISSING_SUBFIELD_A);
    var mappingRules = loadMappingRules();
    var validator = getValidator();

    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), new MappingParameters(), mappingRules);
      instance.getContributors()
        .forEach(Assertions::assertNull);
      assertTrue(validator.validate(instance).isEmpty());
    }
  }

  @Test
  void testMarcToInstancePrecedingTitles() throws IOException {
    var reader = newMarcReader(PRECEDING_FILE_PATH);
    var mappingRules = loadMappingRules();
    var validator = getValidator();

    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), new MappingParameters(), mappingRules);
      instance.getSucceedingTitles()
        .forEach(succeedingTitle -> {
          assertNotNull(succeedingTitle.getTitle());
          succeedingTitle.getIdentifiers().forEach(id -> {
            assertNotNull(id.getIdentifierTypeId());
            assertNotNull(id.getValue());
          });
        });
      instance.getPrecedingTitles()
        .forEach(precedingTitle -> {
          assertNotNull(precedingTitle.getTitle());
          precedingTitle.getIdentifiers().forEach(id -> {
            assertNotNull(id.getIdentifierTypeId());
            assertNotNull(id.getValue());
          });
        });
      assertTrue(validator.validate(instance).isEmpty());
    }
  }

  @Test
  void testMarcToInstanceNotMappedSubFields() throws IOException {
    var reader = newMarcReader(BIB_WITH_NOT_MAPPED_590_SUBFIELD);
    var mappingRules = loadMappingRules();
    var validator = getValidator();

    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), new MappingParameters(), mappingRules);
      assertNotNull(instance.getTitle());
      assertNotNull(instance.getSource());
      assertNotNull(instance.getNotes());
      assertEquals(1, instance.getNotes().size());
      assertEquals("Adaptation of Xi xiang ji by Wang Shifu", instance.getNotes().getFirst().getNote());
      assertTrue(validator.validate(instance).isEmpty());
    }
  }

  @Test
  void testMarcToInstanceResourceTypeIdMapping() throws IOException {
    var reader = newMarcReader(BIB_WITH_RESOURCE_TYPE_SUBFIELD_VALUE);
    var mappingRules = loadMappingRules();
    List<InstanceType> instanceTypes = loadReferenceData(DEFAULT_INSTANCE_TYPES_PATH, InstanceType[].class);
    var mappingParameters = new MappingParameters().withInstanceTypes(instanceTypes);

    var mappedInstances = mapAllRecords(reader, mappingParameters, mappingRules, getValidator());

    assertFalse(mappedInstances.isEmpty());
    assertEquals(4, mappedInstances.size());
    assertEquals(TXT_INSTANCE_TYPE_ID, mappedInstances.getFirst().getInstanceTypeId());
    assertEquals(TXT_INSTANCE_TYPE_ID, mappedInstances.get(1).getInstanceTypeId());
    assertEquals(TXT_INSTANCE_TYPE_ID, mappedInstances.get(2).getInstanceTypeId());
    assertEquals(UNSPECIFIED_INSTANCE_TYPE_ID, mappedInstances.get(3).getInstanceTypeId());
  }

  @Test
  void testMarcToInstanceFormatIdMapping() throws IOException {
    var reader = newMarcReader(BIB_WITH_FORMAT_SUBFIELD_VALUE);
    var mappingRules = loadMappingRules();
    List<InstanceFormat> instanceFormats =
      loadReferenceData(DEFAULT_INSTANCE_FORMAT_IDENTIFIERS, InstanceFormat[].class);
    var mappingParameters = new MappingParameters().withInstanceFormats(instanceFormats);

    var mappedInstances = mapAllRecords(reader, mappingParameters, mappingRules, getValidator());

    assertFalse(mappedInstances.isEmpty());
    assertEquals(5, mappedInstances.size());
    String expectedFirstFormatId = "2e48e713-17f3-4c13-a9f8-23845bb210a4";

    mappedInstances.forEach(mappedInstance -> {
      assertNotNull(mappedInstance.getInstanceFormatIds());
      assertEquals(expectedFirstFormatId, mappedInstance.getInstanceFormatIds().getFirst());
    });

    List<String> expectedMultipleFormatIds = List.of(
      "2e48e713-17f3-4c13-a9f8-23845bb210a4",
      "e8b311a6-3b21-43f2-a269-dd9310cb2d0e",
      "2b94c631-fca9-4892-a730-03ee529ffe27"
    );
    assertEquals(expectedMultipleFormatIds, mappedInstances.get(4).getInstanceFormatIds());
  }

  @Test
  void testMarcToInstanceWithRepeatableIsbn() throws IOException {
    final String isbnIdentifierId = "8261054f-be78-422d-bd51-4ed9f33c3422";
    final String invalidIsbnIdentifierId = "fcca2643-406a-482a-b760-7a7f8aec640e";
    final List<Map.Entry<String, String>> expectedResults = List.of(
      Map.entry("9780471622673 (acid-free paper)", isbnIdentifierId),
      Map.entry("0471725331 (electronic bk.)", isbnIdentifierId),
      Map.entry("9780471725336 (electronic bk.)", invalidIsbnIdentifierId),
      Map.entry("0471725323 (electronic bk.)", invalidIsbnIdentifierId),
      Map.entry("9780471725329 (electronic bk.)", isbnIdentifierId),
      Map.entry("0471622672 (acid-free paper)", invalidIsbnIdentifierId));

    var reader = newMarcReader(BIB_WITH_REPEATED_020_SUBFIELDS);
    var mappingRules = loadMappingRules();
    List<IdentifierType> identifierTypes =
      loadReferenceData(DEFAULT_RESOURCE_IDENTIFIERS_TYPES_PATH, IdentifierType[].class);
    var mappingParameters = new MappingParameters().withIdentifierTypes(identifierTypes);

    var mappedInstances = mapAllRecords(reader, mappingParameters, mappingRules, getValidator());

    assertFalse(mappedInstances.isEmpty());
    assertEquals(1, mappedInstances.size());
    List<Identifier> identifiers = mappedInstances.getFirst().getIdentifiers();
    assertEquals(6, identifiers.size());
    IntStream.range(0, expectedResults.size()).forEach(index -> {
      Map.Entry<String, String> expected = expectedResults.get(index);
      Identifier actual = identifiers.get(index);
      assertEquals(expected.getValue(), actual.getIdentifierTypeId());
      assertEquals(expected.getKey(), actual.getValue());
    });
  }

  @Test
  void testMarcToInstanceWithRepeatableSubjects() throws IOException {
    final List<Subject> expectedResults = getExpectedRepeatableSubjects();

    var reader = newMarcReader(BIB_WITH_REPEATED_600_SUBFIELDS);
    var mappingRules = loadMappingRules();
    List<SubjectSource> subjectSources = loadReferenceData(DEFAULT_SUBJECT_SOURCES_PATH, SubjectSource[].class);
    List<SubjectType> subjectTypes = loadReferenceData(DEFAULT_SUBJECT_TYPES_PATH, SubjectType[].class);
    var mappingParameters = new MappingParameters().withSubjectSources(subjectSources).withSubjectTypes(subjectTypes);

    var mappedInstances = mapAllRecords(reader, mappingParameters, mappingRules, getValidator());

    assertFalse(mappedInstances.isEmpty());
    assertEquals(1, mappedInstances.size());

    Set<Subject> subjects = mappedInstances.getFirst().getSubjects();
    assertEquals(17, subjects.size());

    Iterator<Subject> iterator = subjects.iterator();
    expectedResults.forEach(expected -> {
      Subject actual = iterator.next();
      assertEquals(expected.getValue(), actual.getValue());
      assertEquals(expected.getSourceId(), actual.getSourceId());
      assertEquals(expected.getTypeId(), actual.getTypeId());
    });
  }

  @Test
  void testMarcToInstanceWith008Date() throws IOException {
    var reader = newMarcReader(BIB_WITH_008_DATE);
    var mappingRules = loadMappingRules();
    List<InstanceDateType> instanceDateTypes =
      loadReferenceData(DEFAULT_INSTANCE_DATE_TYPES_PATH, InstanceDateType[].class);
    var mappingParameters = new MappingParameters().withInstanceDateTypes(instanceDateTypes);

    var mappedInstances = mapAllRecords(reader, mappingParameters, mappingRules, getValidator());

    assertFalse(mappedInstances.isEmpty());
    assertEquals(1, mappedInstances.size());

    Instance mappedInstance = mappedInstances.getFirst();
    assertNotNull(mappedInstance.getId());

    assertEquals("1991", mappedInstances.getFirst().getDates().getDate1());
    assertEquals("0101", mappedInstances.getFirst().getDates().getDate2());
    assertEquals("24a506e8-2a92-4ecc-bd09-ff849321fd5a", mappedInstances.getFirst().getDates().getDateTypeId());
  }

  @Test
  void testMarcToInstanceWithDeletedLeader() throws IOException {
    var reader = newMarcReader(BIB_WITH_DELETED_LEADER);
    var mappingRules = loadMappingRules();
    List<InstanceDateType> instanceDateTypes =
      loadReferenceData(DEFAULT_INSTANCE_DATE_TYPES_PATH, InstanceDateType[].class);
    var mappingParameters = new MappingParameters().withInstanceDateTypes(instanceDateTypes);

    var mappedInstances = mapAllRecords(reader, mappingParameters, mappingRules, getValidator());

    assertFalse(mappedInstances.isEmpty());
    assertEquals(1, mappedInstances.size());

    Instance mappedInstance = mappedInstances.getFirst();
    assertNotNull(mappedInstance.getId());

    assertEquals(true, mappedInstances.getFirst().getDeleted());
    assertEquals(true, mappedInstances.getFirst().getStaffSuppress());
    assertEquals(true, mappedInstances.getFirst().getDiscoverySuppress());
  }

  @Test
  void testMarcToInstanceWithEmpty008Date() throws IOException {
    var reader = newMarcReader(BIB_WITHOUT_008_DATE);
    var mappingRules = loadMappingRules();
    List<InstanceDateType> instanceDateTypes =
      loadReferenceData(DEFAULT_INSTANCE_DATE_TYPES_PATH, InstanceDateType[].class);
    var mappingParameters = new MappingParameters().withInstanceDateTypes(instanceDateTypes);

    var mappedInstances = mapAllRecords(reader, mappingParameters, mappingRules, getValidator());

    assertFalse(mappedInstances.isEmpty());
    assertEquals(1, mappedInstances.size());

    Instance mappedInstance = mappedInstances.getFirst();
    assertNotNull(mappedInstance.getId());

    assertNull(mappedInstances.getFirst().getDates().getDate1());
    assertNull(mappedInstances.getFirst().getDates().getDate2());
    assertEquals("77a09c3c-37bd-4ad3-aae4-9d86fc1b33d8", mappedInstances.getFirst().getDates().getDateTypeId());
  }

  @Test
  void testMarcToInstanceWithEmpty008Field() throws IOException {
    var reader = newMarcReader(BIB_WITH_INVALID_008_FIELD);
    var mappingRules = loadMappingRules();
    List<InstanceDateType> instanceDateTypes =
      loadReferenceData(DEFAULT_INSTANCE_DATE_TYPES_PATH, InstanceDateType[].class);
    var mappingParameters = new MappingParameters().withInstanceDateTypes(instanceDateTypes);

    var mappedInstances = mapAllRecords(reader, mappingParameters, mappingRules, getValidator());

    assertFalse(mappedInstances.isEmpty());
    assertEquals(1, mappedInstances.size());

    Instance mappedInstance = mappedInstances.getFirst();
    assertNotNull(mappedInstance.getId());

    assertNull(mappedInstances.getFirst().getDates());
  }

  @Test
  void testMarcToInstanceWithRepeatableSubjectsMappedWithTypeButWithoutIndicators() throws IOException {
    final String firstSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b1";
    final String secondSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b2";
    final String thirdSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b3";
    final String fourthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b4";
    final String fifthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b5";
    final String sixthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b6";
    final String seventhSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b7";
    final String eighthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b8";
    final String ninthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff511";

    final List<Subject> expectedResults = List.of(
      new Subject().withValue("Test 600.2 subject").withTypeId(firstSubjectTypeId),
      new Subject().withValue("Test 610 subject").withTypeId(secondSubjectTypeId),
      new Subject().withValue("Test 611 subject").withTypeId(thirdSubjectTypeId),
      new Subject().withValue("Test 630 subject").withTypeId(fourthSubjectTypeId),
      new Subject().withValue("Test 647 subject").withTypeId(fifthSubjectTypeId),
      new Subject().withValue("Test 648 subject").withTypeId(sixthSubjectTypeId),
      new Subject().withValue("Test 650 subject").withTypeId(seventhSubjectTypeId),
      new Subject().withValue("Test 651 subject").withTypeId(eighthSubjectTypeId),
      new Subject().withValue("Test 655 subject").withTypeId(ninthSubjectTypeId)
    );

    var reader = newMarcReader(BIB_WITH_REPEATED_600_SUBFIELD_AND_EMPTY_INDICATOR);
    var mappingRules = loadMappingRules();
    List<SubjectSource> subjectSources = loadReferenceData(DEFAULT_SUBJECT_SOURCES_PATH, SubjectSource[].class);
    List<SubjectType> subjectTypes = loadReferenceData(DEFAULT_SUBJECT_TYPES_PATH, SubjectType[].class);
    var mappingParameters = new MappingParameters().withSubjectSources(subjectSources).withSubjectTypes(subjectTypes);

    var mappedInstances = mapAllRecords(reader, mappingParameters, mappingRules, getValidator());

    assertFalse(mappedInstances.isEmpty());
    assertEquals(1, mappedInstances.size());

    Set<Subject> subjects = mappedInstances.getFirst().getSubjects();
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
  void testMarcToSubjectSourceIdMappingByCodeFrom2Subfield() throws IOException {
    var reader = newMarcReader(BIB_WITH_SUBJECT_SOURCES_CODE_IN_2_SUBFIELD);
    var mappingRules = loadMappingRules();
    List<SubjectSource> subjectSources = new ObjectMapper()
      .readValue(new File(DEFAULT_SUBJECT_SOURCES_PATH), new TypeReference<>() { });
    var mappingParameters = new MappingParameters().withSubjectSources(subjectSources);

    Instance instance = mapSingleRecord(reader, mappingParameters, mappingRules);

    assertNotNull(instance.getSubjects());
    assertEquals(9, instance.getSubjects().size());
    Map<String, String> subjectValueToSourceId = Map.of(
      "Subject heading 600", "e894d0dc-621d-4b1d-98f6-6f7120eb0d40",
      "Subject heading 610", "e894d0dc-621d-4b1d-98f6-6f7120eb0d41",
      "Subject heading 611", "e894d0dc-621d-4b1d-98f6-6f7120eb0d42",
      "Subject heading 630", "e894d0dc-621d-4b1d-98f6-6f7120eb0d45",
      "Subject heading 647", "e894d0dc-621d-4b1d-98f6-6f7120eb0d46",
      "Subject heading 648", "e894d0dc-621d-4b1d-98f6-6f7120eb0d40",
      "Subject heading 650", "e894d0dc-621d-4b1d-98f6-6f7120eb0d41",
      "Subject heading 651", "e894d0dc-621d-4b1d-98f6-6f7120eb0d42",
      "Subject heading 655", "e894d0dc-621d-4b1d-98f6-6f7120eb0d45"
    );
    instance.getSubjects().forEach(subject -> {
      assertNotNull(subject.getValue());
      assertEquals(subjectValueToSourceId.get(subject.getValue()), subject.getSourceId());
    });
  }

  @Test
  void testMarc720ToInstanceContributors() throws IOException {
    var reader = newMarcReader(BIB_WITH_720_FIELDS);
    var mappingRules = loadMappingRules();

    List<ContributorType> contributorTypes = List.of(
      new ContributorType().withName("Author").withCode("aut").withId("1"),
      new ContributorType().withName("Editor").withCode("edi").withId("2"));

    List<ContributorNameType> contributorNameTypes = List.of(
      new ContributorNameType().withName("Personal name").withId("1"),
      new ContributorNameType().withName("Corporate name").withId("2"));

    var mappingParameters = new MappingParameters().withContributorTypes(contributorTypes)
      .withContributorNameTypes(contributorNameTypes);

    Instance instance = mapSingleRecord(reader, mappingParameters, mappingRules);

    assertNotNull(instance.getSource());
    assertEquals(6, instance.getContributors().size());
    // 720 \\$aBoguslawski, Pawel$4aut$4edt should match by first $4 subfield and set contributorTypeId
    assertEquals("Boguslawski, Pawel", instance.getContributors().getFirst().getName());
    assertEquals("1", instance.getContributors().getFirst().getContributorTypeId());
    assertNull(instance.getContributors().getFirst().getContributorTypeText());
    assertEquals("1", instance.getContributors().getFirst().getContributorNameTypeId());

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

    // 720 1\$aKURIHARA, N.$edata contact$ecreator should set data from first $e to the "contributorTypeText" if all $e
    // don't match
    assertEquals("KURIHARA, N.", instance.getContributors().get(4).getName());
    assertNull(instance.getContributors().get(4).getContributorTypeId());
    assertEquals("data contact", instance.getContributors().get(4).getContributorTypeText());
    assertEquals("1", instance.getContributors().get(4).getContributorNameTypeId());

    // 720 2\$aLondon Symphony Orchestra.$eoth$4aut should set "getContributorNameTypeId" as Corporate name if ind1 == 2
    assertEquals("London Symphony Orchestra", instance.getContributors().get(5).getName());
    assertEquals("1", instance.getContributors().get(5).getContributorTypeId());
    assertNull(instance.getContributors().get(5).getContributorTypeText());
    assertEquals("2", instance.getContributors().get(5).getContributorNameTypeId());

    Set<ConstraintViolation<Instance>> violations = getValidator().validate(instance);
    assertTrue(violations.isEmpty());
  }

  @Test
  void testMarcAlternativeMappingForInstanceContributors() throws IOException {
    var reader = newMarcReader(BIB_WITH_FIELDS_FOR_ALTERNATIVE_MAPPING);
    var mappingRules = loadMappingRules();

    List<ContributorType> contributorTypes = List.of(
      new ContributorType().withName("Author").withCode("aut").withId("1"),
      new ContributorType().withName("Editor").withCode("edi").withId("2"));

    List<ContributorNameType> contributorNameTypes = List.of(
      new ContributorNameType().withName("Personal name").withId("1"),
      new ContributorNameType().withName("Corporate name").withId("2"),
      new ContributorNameType().withName("Meeting name").withId("3"));

    var mappingParameters = new MappingParameters().withContributorTypes(contributorTypes)
      .withContributorNameTypes(contributorNameTypes);
    var validator = getValidator();

    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), mappingParameters, mappingRules);
      assertNotNull(instance.getSource());
      assertEquals(15, instance.getContributors().size());
      assertContributor(instance.getContributors().getFirst(), "Chin, Staceyann, 1972-", "1", null, "1");
      assertContributor(instance.getContributors().get(1), "Oklahoma. Dept. of Highways", "1", null, "2");
      assertContributor(instance.getContributors().get(2), "International Conference on Business History",
        "1", null, "3");
      assertContributor(instance.getContributors().get(3), "Boguslawski, Pawel", "1", null, "1");
      assertContributor(instance.getContributors().get(4), "CHUJO, T.", "2", null, "1");
      assertContributor(instance.getContributors().get(5), "Abdul Rahman, Alias", "2", null, "1");
      assertContributor(instance.getContributors().get(6), "Gold, Christopher", "2", null, "1");
      assertContributor(instance.getContributors().get(7), "KURIHARA, N.", null, "data contact", "1");
      assertContributor(instance.getContributors().get(8), "London Symphony Orchestra", "1", null, "1");
      assertContributor(instance.getContributors().get(9), "Boguslawski, Pawel", "1", null, "3");
      assertContributor(instance.getContributors().get(10), "CHUJO, T.", "2", null, "3");
      assertContributor(instance.getContributors().get(11), "Abdul Rahman, Alias", "2", null, "3");
      assertContributor(instance.getContributors().get(12), "Gold, Christopher", "2", null, "3");
      assertContributor(instance.getContributors().get(13), "KURIHARA, N.", null, "data contact", "3");
      assertContributor(instance.getContributors().get(14), "London Symphony Orchestra", "1", null, "3");

      assertTrue(validator.validate(instance).isEmpty());
    }
  }

  @Test
  void testMarcAlternativeMappingForInstanceContributorsWithPunctuations() throws IOException {
    var reader = newMarcReader(BIB_WITH_FIELDS_FOR_ALTERNATIVE_MAPPING_WITH_PUNCTUATIONS);
    var mappingRules = loadMappingRules();

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

    var mappingParameters = new MappingParameters().withContributorTypes(contributorTypes)
      .withContributorNameTypes(contributorNameTypes);
    var validator = getValidator();

    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), mappingParameters, mappingRules);
      assertNotNull(instance.getSource());
      assertEquals(10, instance.getContributors().size());

      // 100 1\$aKani, John,$econceptor;$ecourt report should match by first $e subfield and set contributorTypeId to 3
      assertEquals("Kani, John", instance.getContributors().getFirst().getName());
      assertEquals("3", instance.getContributors().getFirst().getContributorTypeId());
      assertNull(instance.getContributors().getFirst().getContributorTypeText());
      assertEquals("1", instance.getContributors().getFirst().getContributorNameTypeId());

      // 110 2\$aBuena Vista Corporate (Firm),$efilm distributor. should remove comma at the end of the name, match by
      // first $e subfield and set contributorTypeId to 5
      assertEquals("Buena Vista Corporate (Firm)", instance.getContributors().get(1).getName());
      assertEquals("5", instance.getContributors().get(1).getContributorTypeId());
      assertNull(instance.getContributors().get(1).getContributorTypeText());
      assertEquals("2", instance.getContributors().get(1).getContributorNameTypeId());

      // 111 2\$aSuperheroes,$jassociated name;$jdepicted. should remove comma at the end of the name, match by first
      // $j subfield and set contributorTypeId to 6
      assertEquals("Superheroes", instance.getContributors().get(2).getName());
      assertEquals("6", instance.getContributors().get(2).getContributorTypeId());
      assertNull(instance.getContributors().get(2).getContributorTypeText());
      assertEquals("3", instance.getContributors().get(2).getContributorNameTypeId());

      // 700 1\$aBrown, Sterling K.,$eactress;$einterviewer. should remove comma at the end of the name, match by
      // second $e subfield and set contributorTypeId to 8
      assertEquals("Brown, Sterling K.", instance.getContributors().get(3).getName());
      assertEquals("8", instance.getContributors().get(3).getContributorTypeId());
      assertNull(instance.getContributors().get(3).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(3).getContributorNameTypeId());

      // 700 1\$aBrown, Sterling K,.$eactress;$einterviewer. should remove comma at the end of the name, match by
      // second $e subfield and set contributorTypeId to 8
      assertEquals("Brown, Sterling K.", instance.getContributors().get(4).getName());
      assertEquals("8", instance.getContributors().get(4).getContributorTypeId());
      assertNull(instance.getContributors().get(4).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(4).getContributorNameTypeId());

      // 700 1\$aBrown, Sterling K-$$einterviewer. should NOT remove the hyphen at the end of the name, match by second
      // $e subfield and set contributorTypeId to 8
      assertEquals("Brown, Sterling K-", instance.getContributors().get(5).getName());
      assertEquals("8", instance.getContributors().get(5).getContributorTypeId());
      assertNull(instance.getContributors().get(5).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(5).getContributorNameTypeId());

      // 700 1\$aMorrison, Rachel$c(Cinematographer),$edirector of photorgaphy. should remove comma at the end of the
      // name(subfield a+c), not match by $e subfield and set it as contributorTypeText to 8
      assertEquals("Morrison, Rachel (Cinematographer)", instance.getContributors().get(6).getName());
      assertNull(instance.getContributors().get(6).getContributorTypeId());
      assertEquals("director of photorgaphy.", instance.getContributors().get(6).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(6).getContributorNameTypeId());

      // 700 1\$aMorrison, Rachel$c(Cinematographer),$eeAuthor of introduction, etc. should remove comma at the end of
      // the name(subfield a+c), match by $e subfield and set contributorTypeId to 9
      assertEquals("Morrison, Rachel (Cinematographer)", instance.getContributors().get(7).getName());
      assertEquals("9", instance.getContributors().get(7).getContributorTypeId());
      assertNull(instance.getContributors().get(7).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(7).getContributorNameTypeId());

      // 700 1\$aMorrison, Rachel$c(Cinematographer),$eeAuthor of introduction, etc should remove comma at the end of
      // the name(subfield a+c), match by $e subfield and set contributorTypeId to 9
      assertEquals("Morrison, Rachel (Cinematographer)", instance.getContributors().get(8).getName());
      assertEquals("9", instance.getContributors().get(8).getContributorTypeId());
      assertNull(instance.getContributors().get(8).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(8).getContributorNameTypeId());

      // 700 1\$aWright, Letitia,$d1993-$eauthor of introduction, etc.;$eactor. should remove comma at the end of the
      // name(subfield a+c), match by $e author of introduction, etc. subfield and set contributorTypeId to 9
      assertEquals("Wright, Letitia, 1993-", instance.getContributors().get(9).getName());
      assertEquals("9", instance.getContributors().get(9).getContributorTypeId());
      assertNull(instance.getContributors().get(9).getContributorTypeText());
      assertEquals("1", instance.getContributors().get(9).getContributorNameTypeId());

      assertTrue(validator.validate(instance).isEmpty());
    }
  }

  @Test
  void testMarcToInstanceForInstanceTypeIds() throws IOException {
    var reader = newMarcReader(BIB_WITH_010Z_SUBFIELD);
    var mappingRules = loadMappingRules();
    List<InstanceType> instanceTypes = loadReferenceData(DEFAULT_INSTANCE_TYPES_PATH, InstanceType[].class);
    var mappingParameters = new MappingParameters().withInstanceTypes(instanceTypes);

    var mappedInstances = mapAllRecords(reader, mappingParameters, mappingRules, getValidator());

    assertFalse(mappedInstances.isEmpty());
    assertEquals(1, mappedInstances.size());
    int expectedSizeOfIdentifiers = 7;
    assertEquals(expectedSizeOfIdentifiers, mappedInstances.getFirst().getIdentifiers().size());
    mappedInstances.getFirst().getIdentifiers().forEach(Assertions::assertNotNull);

    var identifiers = mappedInstances.getFirst().getIdentifiers();
    String expected010SubfieldZ = "3025698745";
    assertTrue(identifiers.stream().map(Identifier::getValue)
      .anyMatch(actualValue -> actualValue.equals(expected010SubfieldZ)));
  }

  /**
   * Creates a {@link MarcReader} over the MARC records stored in the file at the given path.
   */
  private static MarcReader newMarcReader(String path) throws IOException {
    return new MarcStreamReader(
      new ByteArrayInputStream(TestUtil.readFileFromPath(path).getBytes(StandardCharsets.UTF_8)));
  }

  private static JsonObject loadMappingRules() throws IOException {
    return new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));
  }

  /**
   * Converts a single MARC {@link Record} into the {@link JsonObject} representation expected by the mapper.
   */
  private static JsonObject marcRecordToJson(Record marcRecord) {
    var os = new ByteArrayOutputStream();
    var writer = new MarcJsonWriter(os);
    writer.write(marcRecord);
    return new JsonObject(os.toString());
  }

  private static <T> List<T> loadReferenceData(String path, Class<T[]> arrayType) throws IOException {
    String raw = TestUtil.readFileFromPath(path);
    return List.of(new ObjectMapper().readValue(raw, arrayType));
  }

  /**
   * Maps every record left in the reader to an {@link Instance}, validating each one against the given validator,
   * and returns all mapped instances in order for further assertions.
   */
  private List<Instance> mapAllRecords(MarcReader reader, MappingParameters mappingParameters,
                                       JsonObject mappingRules, Validator validator) throws IOException {
    List<Instance> mappedInstances = new ArrayList<>();
    while (reader.hasNext()) {
      Instance instance = mapper.mapRecord(marcRecordToJson(reader.next()), mappingParameters, mappingRules);
      mappedInstances.add(instance);
      assertTrue(validator.validate(instance).isEmpty());
    }
    return mappedInstances;
  }

  /**
   * Maps the next single record from the reader to an {@link Instance}, asserting that the reader is not empty.
   */
  private Instance mapSingleRecord(MarcReader reader, MappingParameters mappingParameters, JsonObject mappingRules)
    throws IOException {
    assertTrue(reader.hasNext());
    return mapper.mapRecord(marcRecordToJson(reader.next()), mappingParameters, mappingRules);
  }

  private Validator getValidator() {
    try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
      return factory.getValidator();
    }
  }

  private static void assertContributor(Contributor contributor, String expectedName,
                                        String expectedContributorTypeId, String expectedContributorTypeText,
                                        String expectedContributorNameTypeId) {
    assertEquals(expectedName, contributor.getName());
    assertEquals(expectedContributorTypeId, contributor.getContributorTypeId());
    assertEquals(expectedContributorTypeText, contributor.getContributorTypeText());
    assertEquals(expectedContributorNameTypeId, contributor.getContributorNameTypeId());
  }

  private static List<Subject> getExpectedRepeatableSubjects() {
    final String firstLibrarySourceId = "e894d0dc-621d-4b1d-98f6-6f7120eb0d40";
    final String secondLibrarySourceId = "e894d0dc-621d-4b1d-98f6-6f7120eb0d41";
    final String thirdLibrarySourceId = "e894d0dc-621d-4b1d-98f6-6f7120eb0d42";
    final String fourthLibrarySourceId = "e894d0dc-621d-4b1d-98f6-6f7120eb0d43";
    final String fifthLibrarySourceId = "e894d0dc-621d-4b1d-98f6-6f7120eb0d44";
    final String sixthLibrarySourceId = "e894d0dc-621d-4b1d-98f6-6f7120eb0d45";
    final String seventhLibrarySourceId = "e894d0dc-621d-4b1d-98f6-6f7120eb0d46";

    final String firstSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b1";
    final String secondSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b2";
    final String thirdSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b3";
    final String fourthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b4";
    final String fifthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b5";
    final String sixthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b6";
    final String seventhSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b7";
    final String eighthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b8";
    final String ninthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff511";
    final String tenthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff5b9";
    final String eleventhSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff510";
    final String twelfthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff512";
    final String thirteenthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff513";
    final String fourteenthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff514";
    final String fifteenthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff515";
    final String sixteenthSubjectTypeId = "d6488f88-1e74-40ce-81b5-b19a928ff516";

    return List.of(
      new Subject().withValue("Testing 600 subject Testing 600b subject").withSourceId(firstLibrarySourceId)
        .withTypeId(firstSubjectTypeId),
      new Subject().withValue("Test 600.2 subject").withSourceId(fifthLibrarySourceId)
        .withTypeId(firstSubjectTypeId),
      new Subject().withValue("Test 610 subject").withSourceId(thirdLibrarySourceId)
        .withTypeId(secondSubjectTypeId),
      new Subject().withValue("Test 611 subject").withSourceId(fourthLibrarySourceId)
        .withTypeId(thirdSubjectTypeId),
      new Subject().withValue("Test 630 subject").withSourceId(fifthLibrarySourceId)
        .withTypeId(fourthSubjectTypeId),
      new Subject().withValue("Test 647 subject").withSourceId(sixthLibrarySourceId)
        .withTypeId(fifthSubjectTypeId),
      new Subject().withValue("Test 648 subject").withSourceId(sixthLibrarySourceId)
        .withTypeId(sixthSubjectTypeId),
      new Subject().withValue("Test 650 subject").withSourceId(seventhLibrarySourceId)
        .withTypeId(seventhSubjectTypeId),
      new Subject().withValue("Test 651 subject").withSourceId(secondLibrarySourceId)
        .withTypeId(eighthSubjectTypeId),
      new Subject().withValue("Test 653 subject").withSourceId(secondLibrarySourceId)
        .withTypeId(tenthSubjectTypeId),
      new Subject().withValue("Test 654 subject").withSourceId(secondLibrarySourceId)
        .withTypeId(eleventhSubjectTypeId),
      new Subject().withValue("Test 655 subject").withSourceId(secondLibrarySourceId)
        .withTypeId(ninthSubjectTypeId),
      new Subject().withValue("Test 656 subject").withSourceId(secondLibrarySourceId)
        .withTypeId(twelfthSubjectTypeId),
      new Subject().withValue("Test 657 subject").withSourceId(secondLibrarySourceId)
        .withTypeId(thirteenthSubjectTypeId),
      new Subject().withValue("Test 658 subject").withSourceId(secondLibrarySourceId)
        .withTypeId(fourteenthSubjectTypeId),
      new Subject().withValue("Test 662 subject").withSourceId(secondLibrarySourceId)
        .withTypeId(fifteenthSubjectTypeId),
      new Subject().withValue("Test 688 subject").withSourceId(secondLibrarySourceId)
        .withTypeId(sixteenthSubjectTypeId)
    );
  }
}
