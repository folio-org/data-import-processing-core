package org.folio.processing.mapping;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.Instance;
import org.folio.processing.TestUtil;
import org.folio.processing.mapping.defaultmapper.RecordToInstanceMapper;
import org.folio.processing.mapping.defaultmapper.RecordToInstanceMapperBuilder;
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
import java.util.Set;

@RunWith(JUnit4.class)
public class MappingTest {

  private RecordToInstanceMapper mapper = RecordToInstanceMapperBuilder.buildMapper("MARC");

  private static final String INSTANCES_PATH = "src/test/resources/org/folio/processing/mapping/instances.json";
  private static final String BIBS_PATH = "src/test/resources/org/folio/processing/mapping/CornellFOLIOExemplars_Bibs.mrc";
  private static final String BIBS_ERRORS_PATH = "src/test/resources/org/folio/processing/mapping/test1_err.mrc";
  private static final String BIB_WITH_REPEATED_SUBFIELDS_PATH = "src/test/resources/org/folio/processing/mapping/336_repeated_subfields.mrc";
  private static final String DEFAULT_MAPPING_RULES_PATH = "src/test/resources/org/folio/processing/mapping/rules.json";
  private static final String STUB_FIELD_TYPE_ID = "fe19bae4-da28-472b-be90-d442e2428ead";
  private static final String BIB_WITH_MISSING_URI = "src/test/resources/org/folio/processing/mapping/856_missing_uri.mrc";

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
      JsonObject marc = new JsonObject(new String(os.toByteArray()));
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
      JsonObject marc = new JsonObject(new String(os.toByteArray()));
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
      JsonObject marc = new JsonObject(new String(os.toByteArray()));
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
  public void testMarcToInstanceRemoveElectronicAccessEntriesWithNoUri() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIB_WITH_MISSING_URI).getBytes(StandardCharsets.UTF_8)));
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MAPPING_RULES_PATH));

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(new String(os.toByteArray()));
      Instance instance = mapper.mapRecord(marc, new MappingParameters(), mappingRules);
      instance.getElectronicAccess()
        .forEach(electronicAccess ->
          Assert.assertNotNull(electronicAccess.getUri()));
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      Assert.assertTrue(violations.isEmpty());
    }

  }
}
