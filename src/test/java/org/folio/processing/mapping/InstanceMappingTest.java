package org.folio.processing.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.Identifier;
import org.folio.IdentifierType;
import org.folio.Instance;
import org.folio.InstanceType;
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

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

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
  private static final String BIB_WITH_RESOURCE_TYPE_SUBFIELD_VALUE = "src/test/resources/org/folio/processing/mapping/instance/336_subfields_mapping.mrc";

  private static final String DEFAULT_MAPPING_RULES_PATH = "src/test/resources/org/folio/processing/mapping/instance/rules.json";
  private static final String DEFAULT_INSTANCE_TYPES_PATH = "src/test/resources/org/folio/processing/mapping/instance/instanceTypes.json";
  private static final String DEFAULT_RESOURCE_IDENTIFIERS_TYPES_PATH = "src/test/resources/org/folio/processing/mapping/instance/resourceIdentifiers.json";
  private static final String STUB_FIELD_TYPE_ID = "fe19bae4-da28-472b-be90-d442e2428ead";
  private static final String TXT_INSTANCE_TYPE_ID = "6312d172-f0cf-40f6-b27d-9fa8feaf332f";
  private static final String UNSPECIFIED_INSTANCE_TYPE_ID = "30fffe0e-e985-4144-b2e2-1e8179bdb41f";
  private static final String BIB_WITH_MISSING_URI = "src/test/resources/org/folio/processing/mapping/instance/856_missing_uri.mrc";

  @Test
  public void testMarcToInstance() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIBS_PATH).getBytes(StandardCharsets.UTF_8)));
    JsonArray instances = new JsonArray(TestUtil.readFileFromPath(INSTANCES_PATH));
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
      Assert.assertNotNull(instance.getTitle());
      Assert.assertNotNull(instance.getSource());
      Assert.assertNotNull(instance.getInstanceTypeId());
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      Assert.assertTrue(violations.isEmpty());
      Assert.assertEquals(instances.getJsonObject(i).encode(), JsonObject.mapFrom(instance).put("id", "0").encode());
      i++;
    }
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
      Assert.assertNotNull(instance.getTitle());
      Assert.assertNotNull(instance.getSource());
      Assert.assertNotNull(instance.getInstanceTypeId());
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      Assert.assertTrue(violations.isEmpty());
      i++;
    }
    Assert.assertEquals(50, i);
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
      Assert.assertNotNull(instance.getTitle());
      Assert.assertNotNull(instance.getSource());
      Assert.assertEquals(STUB_FIELD_TYPE_ID, instance.getInstanceTypeId());
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      Assert.assertTrue(violations.isEmpty());
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
      Assert.assertNotNull(instance.getTitle());
      Assert.assertNotNull(instance.getSource());
      Assert.assertEquals(STUB_FIELD_TYPE_ID, instance.getInstanceTypeId());
      Assert.assertNotNull(instance.getContributors().get(1));
      Assert.assertEquals("fe19bae4-da28-472b-be90-d442e2428ead", instance.getContributors().get(1).getContributorNameTypeId());
      Assert.assertEquals("testingMeetingName", instance.getContributors().get(1).getName());
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      Assert.assertTrue(violations.isEmpty());
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
      Assert.assertNotNull(instance.getTitle());
      Assert.assertNotNull(instance.getSource());
      Assert.assertEquals(STUB_FIELD_TYPE_ID, instance.getInstanceTypeId());
      Assert.assertEquals(3, instance.getAlternativeTitles().size());
      Assert.assertNotNull(instance.getAlternativeTitles().stream().filter(e -> e.getAlternativeTitle().equals("testingAlternativeTitle")).findAny().orElse(null));
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      Assert.assertTrue(violations.isEmpty());
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
      Assert.assertNotNull(instance.getTitle());
      Assert.assertNotNull(instance.getSource());
      Assert.assertEquals(STUB_FIELD_TYPE_ID, instance.getInstanceTypeId());
      Assert.assertNotNull(instance.getSeries());
      Assert.assertEquals(1, instance.getSeries().size());
      Assert.assertNotNull(instance.getSeries().stream().filter(e -> e.equals("testingSeries")).findAny().orElse(null));
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      Assert.assertTrue(violations.isEmpty());
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
      Assert.assertNotNull(instance.getTitle());
      Assert.assertNotNull(instance.getSource());
      Assert.assertNotNull(instance.getNotes());
      Assert.assertEquals(7, instance.getNotes().size());
      Assert.assertEquals("Rare copy: Gift of David Pescovitz and Timothy Daly. 12345", instance.getNotes().get(1).getNote());
      Assert.assertTrue( instance.getNotes().get(1).getStaffOnly());
      Assert.assertEquals("Testing Rare copy: Gift of David Pescovitz and Timothy Daly", instance.getNotes().get(2).getNote());
      Assert.assertTrue( instance.getNotes().get(2).getStaffOnly());
      Assert.assertEquals("Testing Rare copy 3: Gift of David Pescovitz and Timothy Daly. 123", instance.getNotes().get(3).getNote());
      Assert.assertFalse( instance.getNotes().get(3).getStaffOnly());
      Assert.assertEquals("Correspondence relating to the collection may be found in Cornell University Libraries. John M. Echols Collection. Records, #13\\\\6\\\\1973", instance.getNotes().get(4).getNote());
      Assert.assertFalse( instance.getNotes().get(4).getStaffOnly());
      Assert.assertEquals("The note should be marked as stuffOnly", instance.getNotes().get(5).getNote());
      Assert.assertTrue(instance.getNotes().get(5).getStaffOnly());
      Assert.assertEquals("The note should not be marked as stuffOnly", instance.getNotes().get(6).getNote());
      Assert.assertFalse(instance.getNotes().get(6).getStaffOnly());
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      Assert.assertTrue(violations.isEmpty());
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
          Assert.assertNotNull(electronicAccess.getUri()));
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      Assert.assertTrue(violations.isEmpty());
    }
  }

  @Test
  public void testMarcToInstancePrecidingTitles() throws IOException {
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
            Assert.assertNotNull(succeedingTitle.getTitle());
            succeedingTitle.getIdentifiers().forEach(id -> {
              Assert.assertNotNull(id.getIdentifierTypeId());
              Assert.assertNotNull(id.getValue());
            });
          }
        );
      instance.getPrecedingTitles()
        .forEach(precedingTitle ->
          {
            Assert.assertNotNull(precedingTitle.getTitle());
            precedingTitle.getIdentifiers().forEach(id -> {
              Assert.assertNotNull(id.getIdentifierTypeId());
              Assert.assertNotNull(id.getValue());
            });
          }
        );
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      Assert.assertTrue(violations.isEmpty());
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
      Assert.assertNotNull(instance.getTitle());
      Assert.assertNotNull(instance.getSource());
      Assert.assertNotNull(instance.getNotes());
      Assert.assertEquals(1, instance.getNotes().size());
      Assert.assertEquals("Adaptation of Xi xiang ji by Wang Shifu", instance.getNotes().get(0).getNote());
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      Assert.assertTrue(violations.isEmpty());
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
      Assert.assertTrue(violations.isEmpty());
    }
    Assert.assertFalse(mappedInstances.isEmpty());
    Assert.assertEquals(4, mappedInstances.size());
    Assert.assertEquals(TXT_INSTANCE_TYPE_ID, mappedInstances.get(0).getInstanceTypeId());
    Assert.assertEquals(TXT_INSTANCE_TYPE_ID, mappedInstances.get(1).getInstanceTypeId());
    Assert.assertEquals(TXT_INSTANCE_TYPE_ID, mappedInstances.get(2).getInstanceTypeId());
    Assert.assertEquals(UNSPECIFIED_INSTANCE_TYPE_ID, mappedInstances.get(3).getInstanceTypeId());
  }

  @Test
  public void testMarcToInstanceWithRepeatableISBN() throws IOException {
    final String ISBN_IDENTIFIER_ID = "8261054f-be78-422d-bd51-4ed9f33c3422";
    final String INVALID_ISBN_IDENTIFIER_ID = "fcca2643-406a-482a-b760-7a7f8aec640e";
    final List<Map.Entry<String, String>> expectedResults = List.of(
      Map.entry("9780471622673 (acid-free paper)", ISBN_IDENTIFIER_ID),
      Map.entry("0471725331 (electronic bk.)", ISBN_IDENTIFIER_ID),
      Map.entry("9780471725336 (electronic bk.)", INVALID_ISBN_IDENTIFIER_ID),
      Map.entry("0471725323 (electronic bk.)", INVALID_ISBN_IDENTIFIER_ID ),
      Map.entry("9780471725329 (electronic bk.)", ISBN_IDENTIFIER_ID ),
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
      Assert.assertTrue(violations.isEmpty());
    }
    Assert.assertFalse(mappedInstances.isEmpty());
    Assert.assertEquals(1, mappedInstances.size());
    List<Identifier> identifierTypes = mappedInstances.get(0).getIdentifiers();
    Assert.assertEquals(6, identifierTypes.size());
    IntStream.range(0, expectedResults.size()).forEach(index -> {
        Map.Entry<String, String> expected = expectedResults.get(index);
        Identifier actual = identifierTypes.get(index);
        Assert.assertEquals(expected.getValue(), actual.getIdentifierTypeId());
        Assert.assertEquals(expected.getKey(), actual.getValue());
    });
  }

}
