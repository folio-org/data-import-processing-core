package org.folio.processing.mapping.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.Holdings;
import org.folio.Instance;
import org.folio.Location;
import org.folio.MappingProfile;
import org.folio.Organization;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.StatisticalCode;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RepeatableSubfieldMapping;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.EntityType.ORDER;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class MappingManagerUnitTest {

  private final MappingContext mappingContext = new MappingContext();

  @Before
  public void beforeTest() {
    MappingManager.clearReaderFactories();
    MappingManager.clearWriterFactories();
    MappingManager.clearMapperFactories();
  }

  @Test
  public void shouldMap_MarcBibliographicToInstance() throws IOException {
    // given
    MappingProfile mappingProfile = new MappingProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE)
      .withMappingDetails(new MappingDetail()
        .withMappingFields(singletonList(new MappingRule().withPath("indexTitle").withValue("RULE_EXPRESSION").withEnabled("true"))));
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);
    mappingProfileWrapper.setContentType(MAPPING_PROFILE);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
    String givenInstance = new ObjectMapper().writeValueAsString(new TestInstance(UUID.randomUUID().toString()));
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(INSTANCE.value(), givenInstance);
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(mappingProfileWrapper);

    // when
    MappingManager.registerReaderFactory(new TestMarcBibliographicReaderFactory());
    MappingManager.registerWriterFactory(new TestInstanceWriterFactory());
    MappingManager.map(eventPayload, mappingContext);
    // then
    assertNotNull(eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventPayload.getContext().get(INSTANCE.value()));
    TestInstance mappedInstance = new ObjectMapper().readValue(eventPayload.getContext().get(INSTANCE.value()), TestInstance.class);
    assertNotNull(mappedInstance.getId());
    assertNotNull(mappedInstance.getIndexTitle());
  }

  @Test
  public void shouldMap_MarcBibliographicToInstance_checkCopyingLocations() throws IOException {
    // given
    MappingProfile mappingProfile = new MappingProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE)
      .withMappingDetails(new MappingDetail()
        .withMappingFields(singletonList(new MappingRule().withName("permanentLocationId")
          .withPath("indexTitle").withValue("949$l").withEnabled("true"))));
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);
    mappingProfileWrapper.setContentType(MAPPING_PROFILE);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";

    String givenInstance = new ObjectMapper().writeValueAsString(new TestInstance(UUID.randomUUID().toString()));
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(INSTANCE.value(), givenInstance);
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(mappingProfileWrapper);

    String locationId = UUID.randomUUID().toString();
    MappingContext mappingContext = new MappingContext().withMappingParameters(new MappingParameters()
      .withLocations(List.of(new Location()
        .withId(locationId)
        .withCode("CODE"))));

    // when
    MappingManager.registerReaderFactory(new TestMarcBibliographicReaderFactory());
    MappingManager.registerWriterFactory(new TestInstanceWriterFactory());
    MappingManager.map(eventPayload, mappingContext);

    // then
    assertNotNull(eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventPayload.getContext().get(INSTANCE.value()));

    assertNotNull(mappingProfile.getMappingDetails().getMappingFields().get(0));
    assertTrue(mappingProfile.getMappingDetails().getMappingFields().get(0).getAcceptedValues().containsKey(locationId));

    TestInstance mappedInstance = new ObjectMapper().readValue(eventPayload.getContext().get(INSTANCE.value()), TestInstance.class);
    assertNotNull(mappedInstance.getId());
    assertNotNull(mappedInstance.getIndexTitle());
  }

  @Test
  public void shouldMap_MarcBibliographicToOrder_checkOrganizations() throws IOException {
    // given
    MappingProfile mappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(ORDER)
      .withMappingDetails(new MappingDetail()
        .withMappingFields(new ArrayList<>(List.of(
          new MappingRule().withName("vendor").withPath("order.po.vendor").withValue("\"CODE\"").withEnabled("true"),
          new MappingRule().withName("materialSupplier").withPath("order.poLine.physical.materialSupplier").withValue("\"CODE\"").withEnabled("true"),
          new MappingRule().withName("accessProvider").withPath("order.poLine.eresource.accessProvider").withValue("\"CODE\"").withEnabled("true")
        ))));

    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);
    mappingProfileWrapper.setContentType(MAPPING_PROFILE);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";

    String givenOrder = new ObjectMapper().writeValueAsString(new TestOrder(UUID.randomUUID().toString()));
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(ORDER.value(), givenOrder);
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(mappingProfileWrapper);

    String organizationId = UUID.randomUUID().toString();
    MappingContext mappingContext = new MappingContext().withMappingParameters(new MappingParameters()
      .withOrganizations(List.of(new Organization()
        .withId(organizationId)
        .withName("NAME")
        .withCode("CODE")
      )));

    // when
    MappingManager.registerReaderFactory(new TestMarcBibliographicReaderFactory());
    MappingManager.registerWriterFactory(new TestOrderWriterFactory());
    MappingManager.map(eventPayload, mappingContext);

    // then
    assertNotNull(eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventPayload.getContext().get(ORDER.value()));

    for (MappingRule mappingRule : mappingProfile.getMappingDetails().getMappingFields()) {
      assertNotNull(mappingRule);
      assertTrue(mappingRule.getAcceptedValues().containsKey(organizationId));
      String expectedAcceptedValue = String.format("%s (%s)", mappingRule.getValue().replaceAll("\"",""), organizationId);
      assertEquals(expectedAcceptedValue, mappingRule.getAcceptedValues().get(organizationId));
    }
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowException_ifNoReaderEligible() {
    // given
    MappingProfile mappingProfile = new MappingProfile().withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(INSTANCE);
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);
    mappingProfileWrapper.setContentType(MAPPING_PROFILE);

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setCurrentNode(mappingProfileWrapper);
    // when
    MappingManager.registerWriterFactory(new TestInstanceWriterFactory());
    MappingManager.map(eventPayload, mappingContext);
    // then expect runtime exception
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowException_ifNoWriterEligible() {
    // given
    MappingProfile mappingProfile = new MappingProfile().withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(INSTANCE);
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);
    mappingProfileWrapper.setContentType(MAPPING_PROFILE);

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setCurrentNode(mappingProfileWrapper);
    // when
    MappingManager.registerReaderFactory(new TestMarcBibliographicReaderFactory());
    MappingManager.map(eventPayload, mappingContext);
    // then expect runtime exception
  }

  @Test
  public void shouldNotMap_IfNoContentType() throws IOException {
    // given
    MappingProfile mappingProfile = new MappingProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE)
      .withMappingDetails(new MappingDetail()
        .withMappingFields(singletonList(new MappingRule().withPath("indexTitle").withValue("RULE_EXPRESSION"))));
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
    String givenInstance = new ObjectMapper().writeValueAsString(new TestInstance(UUID.randomUUID().toString()));
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(INSTANCE.value(), givenInstance);
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(mappingProfileWrapper);

    // when
    MappingManager.registerReaderFactory(new TestMarcBibliographicReaderFactory());
    MappingManager.registerWriterFactory(new TestInstanceWriterFactory());
    MappingManager.map(eventPayload, mappingContext);
    // then
    assertNotNull(eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventPayload.getContext().get(INSTANCE.value()));
  }

  @Test
  public void shouldMap_MarcBibliographicToInstanceStatisticalCodesFromMarcValue() {
    shouldMap_MarcBibliographicStatisticalCodes(INSTANCE, List.of("abc","bbc"), new Instance(), null, null, List.of(0,1));
  }

  @Test
  public void shouldMap_MarcBibliographicToInstanceStatisticalCodeFromMarcValue() {
    shouldMap_MarcBibliographicStatisticalCodes(INSTANCE, List.of("bbd"), new Instance(), null, null, List.of(1));
  }

  @Test
  public void shouldMap_MarcBibliographicToHoldingsStatisticalCodeFromMarcValue() {
    shouldMap_MarcBibliographicStatisticalCodes(HOLDINGS, List.of("abd"), new Holdings(), null, null, List.of(0));
  }

  @Test
  public void shouldMap_MarcBibliographicToHoldingsStatisticalCode1FromMarcValue() {
    shouldMap_MarcBibliographicStatisticalCodes(HOLDINGS, List.of("abd (abc)"), new Holdings(), null, null, List.of(0));
  }

  @Test
  public void shouldMap_MarcBibliographicToHoldingsStatisticalCode2FromMarcValue() {
    shouldMap_MarcBibliographicStatisticalCodes(HOLDINGS, List.of("bbd (bbc)"), new Holdings(), null, null, List.of(1));
  }

  @Test
  public void shouldMap_MarcBibliographicToItemStatisticalCodeFromMarcValue() {
    shouldMap_MarcBibliographicStatisticalCodes(ITEM, List.of("bbc"), new Holdings(), null, null, List.of(1));
  }

  @Test
  public void shouldMap_MarcBibliographicToItemStatisticalCodesFromMarcValue() {
    shouldMap_MarcBibliographicStatisticalCodes(ITEM, List.of("bbd", "abd (abc)"), new Holdings(), null, null, List.of(1,0));
  }

  @Test
  public void shouldMap_MarcBibliographicToInstanceStatisticalCodesFromStringValue() {
    shouldMap_MarcBibliographicStatisticalCodes(INSTANCE, List.of("abc","bbc"), new Instance(), "\"test\"",
      new HashMap<>(Map.of("uuid1", "test")), List.of(0));
  }

  @Test
  public void shouldMap_MarcBibliographicToHoldingsStatisticalCodesFromStringValue() {
    shouldMap_MarcBibliographicStatisticalCodes(INSTANCE, List.of("abc","bbc"), new Holdings(), "\"test\"",
      new HashMap<>(Map.of("uuid1", "test")), List.of(0));
  }


  private void shouldMap_MarcBibliographicStatisticalCodes(
    EntityType entityType,
    List<String> statisticalCodeValues,
    Object entityInstance,
    String value,
    HashMap<String, String> acceptedValues,
    List<Integer> expectedResultIndexes
  ) {
    List<StatisticalCode> statisticalCodes = List.of(
      new StatisticalCode()
        .withId("uuid1")
        .withCode("abc")
        .withName("abd"),
      new StatisticalCode()
        .withId("uuid2")
        .withCode("bbc")
        .withName("bbd (bbc)")
    );
    MappingProfile mappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(entityType)
      .withMappingDetails(new MappingDetail()
        .withMappingFields(new ArrayList<>(List.of(
          new MappingRule().withName("statisticalCodeIds")
            .withPath("instance.statisticalCodeIds[]")
            .withValue("")
            .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
            .withEnabled("true")
            .withSubfields(new ArrayList<>(List.of(
              new RepeatableSubfieldMapping().withPath("instance.statisticalCodeIds[]")
                .withOrder(0)
                .withFields(List.of(
                  new MappingRule().withName("statisticalCodeId")
                    .withPath("instance.statisticalCodeIds[]")
                    .withValue(value == null ? "971" : value)
                    .withEnabled("true")
                    .withAcceptedValues(acceptedValues)
                ))
            )))
        ))));

    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);
    mappingProfileWrapper.setContentType(MAPPING_PROFILE);

    List<JsonObject> parsedRecordContentFields = new ArrayList<>();
    for (String statisticalCodeValue : statisticalCodeValues) {
      JsonObject field = new JsonObject();
      field.put("971",statisticalCodeValue);
      parsedRecordContentFields.add(field);
    }
    JsonObject parsedRecordContent = new JsonObject();
    parsedRecordContent.put("leader","01314nam  22003851a 4500");
    parsedRecordContent.put("fields",parsedRecordContentFields);
    ParsedRecord parsedRecord = new ParsedRecord()
      .withContent(parsedRecordContent.toString());

    String givenMarcRecord = Json.encode(new Record()
      .withParsedRecord(parsedRecord));
    var entity = new JsonObject();
    entity.put("instance", entityInstance);
    String encodedEntity = entity.encode();
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(entityType.value(), encodedEntity);
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(mappingProfileWrapper);

    MappingContext mappingContext = new MappingContext().withMappingParameters(new MappingParameters()
      .withStatisticalCodes(statisticalCodes));

    MappingManager.registerReaderFactory(new MarcBibReaderFactory());
    MappingManager.registerWriterFactory(new WriterFactory() {
      @Override
      public Writer createWriter() {
        return new JsonBasedWriter(entityType);
      }

      @Override
      public boolean isEligibleForEntityType(EntityType entity) {
        return entityType == entity;
      }
    });
    MappingManager.map(eventPayload, mappingContext);

    assertNotNull(eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventPayload.getContext().get(entityType.value()));

    Map<String,Object> entityResult = (Map) Json.decodeValue(eventPayload.getContext().get(entityType.value()), Map.class).get("instance");
    List<String> statisticalCodeIds =(List) entityResult.get("statisticalCodeIds");
    assertEquals(statisticalCodeIds.size(), expectedResultIndexes.size());
    for (int i = 0; i < expectedResultIndexes.size(); i++) {
      assertEquals(statisticalCodes.get(expectedResultIndexes.get(i)).getId(),statisticalCodeIds.get(i));
    }
  }
}
