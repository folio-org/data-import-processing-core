package org.folio.processing.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.ProfileSnapshotWrapper;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.model.MappingProfile;
import org.folio.processing.mapping.model.Rule;
import org.folio.processing.mapping.reader.TestMarcBibliographicReaderFactory;
import org.folio.processing.mapping.writer.Instance;
import org.folio.processing.mapping.writer.TestInstanceWriterFactory;
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

  @Test
  public void shouldMap_MarcBibliographicToInstance() throws IOException {
    // given
    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
    String givenInstance = new ObjectMapper().writeValueAsString(new Instance(UUID.randomUUID().toString()));
    MappingProfile mappingProfile = new MappingProfile(MARC_BIBLIOGRAPHIC, INSTANCE);
    mappingProfile.getMappingRules().add(new Rule("indexTitle", "RULE_EXPRESSION"));
    ProfileSnapshotWrapper mappingProfileWrapper = new ProfileSnapshotWrapper();
    mappingProfileWrapper.setContent(mappingProfile);

    EventContext eventContext = new EventContext();
    eventContext.putObject(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    eventContext.putObject(INSTANCE.value(), givenInstance);
    eventContext.setCurrentNode(mappingProfileWrapper);
    // when
    MappingManager.registerReaderFactory(new TestMarcBibliographicReaderFactory());
    MappingManager.registerWriterFactory(new TestInstanceWriterFactory());
    MappingManager.map(eventContext);
    // then
    assertNotNull(eventContext.getObjects().get(MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventContext.getObjects().get(INSTANCE.value()));
    Instance mappedInstance = new ObjectMapper().readValue(eventContext.getObjects().get(INSTANCE.value()), Instance.class);
    assertNotNull(mappedInstance.getId());
    assertNotNull(mappedInstance.getIndexTitle());
  }
}
