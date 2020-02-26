package org.folio.processing.mapping.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.model.MappingProfile;
import org.folio.processing.mapping.model.Rule;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.folio.processing.mapping.model.MappingProfile.EntityType.INSTANCE;
import static org.folio.processing.mapping.model.MappingProfile.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.junit.Assert.assertNotNull;

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
    MappingProfile mappingProfile = new MappingProfile(MARC_BIBLIOGRAPHIC, INSTANCE);
    mappingProfile.getMappingRules().add(new Rule("indexTitle", "RULE_EXPRESSION"));
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);
    mappingProfileWrapper.setContentType(MAPPING_PROFILE);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
    String givenInstance = new ObjectMapper().writeValueAsString(new TestInstance(UUID.randomUUID().toString()));
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(INSTANCE.value(), givenInstance);
    eventContext.setContext(context);
    eventContext.setCurrentNode(mappingProfileWrapper);

    // when
    MappingManager.registerReaderFactory(new TestMarcBibliographicReaderFactory());
    MappingManager.registerWriterFactory(new TestInstanceWriterFactory());
    MappingManager.map(eventContext);
    // then
    assertNotNull(eventContext.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventContext.getContext().get(INSTANCE.value()));
    TestInstance mappedInstance = new ObjectMapper().readValue(eventContext.getContext().get(INSTANCE.value()), TestInstance.class);
    assertNotNull(mappedInstance.getId());
    assertNotNull(mappedInstance.getIndexTitle());
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowException_ifNoReaderEligible() throws JsonProcessingException {
    // given
    MappingProfile mappingProfile = new MappingProfile(MARC_BIBLIOGRAPHIC, INSTANCE);
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);
    mappingProfileWrapper.setContentType(MAPPING_PROFILE);

    DataImportEventPayload eventContext = new DataImportEventPayload();
    eventContext.setCurrentNode(mappingProfileWrapper);
    // when
    MappingManager.registerWriterFactory(new TestInstanceWriterFactory());
    MappingManager.map(eventContext);
    // then expect runtime exception
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowException_ifNoWriterEligible() throws JsonProcessingException {
    // given
    MappingProfile mappingProfile = new MappingProfile(MARC_BIBLIOGRAPHIC, INSTANCE);
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);
    mappingProfileWrapper.setContentType(MAPPING_PROFILE);

    DataImportEventPayload eventContext = new DataImportEventPayload();
    eventContext.setCurrentNode(mappingProfileWrapper);
    // when
    MappingManager.registerReaderFactory(new TestMarcBibliographicReaderFactory());
    MappingManager.map(eventContext);
    // then expect runtime exception
  }

  @Test
  public void shouldNotMap_IfNoContentType() throws IOException {
    // given
    MappingProfile mappingProfile = new MappingProfile(MARC_BIBLIOGRAPHIC, INSTANCE);
    mappingProfile.getMappingRules().add(new Rule("indexTitle", "RULE_EXPRESSION"));
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
    String givenInstance = new ObjectMapper().writeValueAsString(new TestInstance(UUID.randomUUID().toString()));
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(INSTANCE.value(), givenInstance);
    eventContext.setContext(context);
    eventContext.setCurrentNode(mappingProfileWrapper);

    // when
    MappingManager.registerReaderFactory(new TestMarcBibliographicReaderFactory());
    MappingManager.registerWriterFactory(new TestInstanceWriterFactory());
    MappingManager.map(eventContext);
    // then
    assertNotNull(eventContext.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventContext.getContext().get(INSTANCE.value()));
  }
}
