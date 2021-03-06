package org.folio.processing.mapping.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.mapping.MappingManager;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class MappingManagerUnitTest {

  @Before
  public void beforeTest() {
    MappingManager.clearReaderFactories();
    MappingManager.clearWriterFactories();
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
    MappingManager.map(eventPayload);
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

    String locationId = UUID.randomUUID().toString();
    String givenMappingParams = "{ \"locations\": [ { \"id\":\"" + locationId + "\", \"code\": \"CODE\" } ] }";
    String givenInstance = new ObjectMapper().writeValueAsString(new TestInstance(UUID.randomUUID().toString()));
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(INSTANCE.value(), givenInstance);
    context.put("MAPPING_PARAMS", givenMappingParams);
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(mappingProfileWrapper);

    // when
    MappingManager.registerReaderFactory(new TestMarcBibliographicReaderFactory());
    MappingManager.registerWriterFactory(new TestInstanceWriterFactory());
    MappingManager.map(eventPayload);
    // then

    assertNotNull(eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventPayload.getContext().get(INSTANCE.value()));

    assertNotNull(mappingProfile.getMappingDetails().getMappingFields().get(0));
    assertTrue(mappingProfile.getMappingDetails().getMappingFields().get(0).getAcceptedValues().containsKey(locationId));

    TestInstance mappedInstance = new ObjectMapper().readValue(eventPayload.getContext().get(INSTANCE.value()), TestInstance.class);
    assertNotNull(mappedInstance.getId());
    assertNotNull(mappedInstance.getIndexTitle());
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowException_ifNoReaderEligible() throws JsonProcessingException {
    // given
    MappingProfile mappingProfile = new MappingProfile().withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(INSTANCE);
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);
    mappingProfileWrapper.setContentType(MAPPING_PROFILE);

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setCurrentNode(mappingProfileWrapper);
    // when
    MappingManager.registerWriterFactory(new TestInstanceWriterFactory());
    MappingManager.map(eventPayload);
    // then expect runtime exception
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowException_ifNoWriterEligible() throws JsonProcessingException {
    // given
    MappingProfile mappingProfile = new MappingProfile().withIncomingRecordType(MARC_BIBLIOGRAPHIC).withExistingRecordType(INSTANCE);
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);
    mappingProfileWrapper.setContentType(MAPPING_PROFILE);

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setCurrentNode(mappingProfileWrapper);
    // when
    MappingManager.registerReaderFactory(new TestMarcBibliographicReaderFactory());
    MappingManager.map(eventPayload);
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
    MappingManager.map(eventPayload);
    // then
    assertNotNull(eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventPayload.getContext().get(INSTANCE.value()));
  }
}
