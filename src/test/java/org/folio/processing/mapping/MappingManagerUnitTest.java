package org.folio.processing.mapping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.ProfileSnapshotWrapper;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.mapper.reader.record.MarcBibReaderFactory;
import org.folio.processing.mapping.model.MappingProfile;
import org.folio.processing.mapping.model.Rule;
import org.folio.processing.mapping.reader.TestMarcBibliographicReaderFactory;
import org.folio.processing.mapping.writer.TestInstance;
import org.folio.processing.mapping.writer.TestInstanceWriterFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.UUID;

import static org.folio.processing.mapping.model.MappingProfile.EntityType.INSTANCE;
import static org.folio.processing.mapping.model.MappingProfile.EntityType.MARC_BIBLIOGRAPHIC;
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

    String givenMarcRecord = "{ \"leader\":\"leadervalue\", \"fields\":[ { \"001\":\"001value\" }, { \"002\":\"002value\" } ] }";
    String givenInstance = new ObjectMapper().writeValueAsString(new TestInstance(UUID.randomUUID().toString()));
    EventContext eventContext = new EventContext();
    eventContext.putObject(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    eventContext.putObject(INSTANCE.value(), givenInstance);
    eventContext.setCurrentNode(mappingProfileWrapper);
    // when
    MappingManager.registerReaderFactory(new MarcBibReaderFactory());
    MappingManager.registerWriterFactory(new TestInstanceWriterFactory());
    MappingManager.map(eventContext);
    // then
    assertNotNull(eventContext.getObjects().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventContext.getObjects().get(INSTANCE.value()));
    TestInstance mappedInstance = new ObjectMapper().readValue(eventContext.getObjects().get(INSTANCE.value()), TestInstance.class);
    assertNotNull(mappedInstance.getId());
    assertNotNull(mappedInstance.getIndexTitle());
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowException_ifNoReaderEligible() throws JsonProcessingException {
    // given
    MappingProfile mappingProfile = new MappingProfile(MARC_BIBLIOGRAPHIC, INSTANCE);
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);

    EventContext eventContext = new EventContext();
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

    EventContext eventContext = new EventContext();
    eventContext.setCurrentNode(mappingProfileWrapper);
    // when
    MappingManager.registerReaderFactory(new TestMarcBibliographicReaderFactory());
    MappingManager.map(eventContext);
    // then expect runtime exception
  }
}
