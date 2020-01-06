package org.folio.processing.matching.manager;

import org.folio.ProfileSnapshotWrapper;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.matching.MatchingManager;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.model.schemas.MatchDetail;
import org.folio.processing.matching.model.schemas.MatchProfile;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static org.folio.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;
import static org.folio.processing.matching.model.schemas.MatchProfile.ExistingRecordType.MARC_BIBLIOGRAPHIC;
import static org.folio.processing.matching.model.schemas.MatchProfile.IncomingRecordType.EDIFACT;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class MatchingManagerTest {

  @Before
  public void beforeTest() {
    MatchValueReaderFactory.clearReaderFactory();
    MatchValueLoaderFactory.clearLoaderFactory();
  }

  @Test
  public void shouldMatch_MarcBibliographicAndEdifact() throws IOException {
    // given
    MatchValueReaderFactory.register(new TestMatchValueReader());
    MatchValueLoaderFactory.register(new TestMatchValueLoader());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withIncomingRecordType(EDIFACT)
      .withMatchDetails(Collections.singletonList(new MatchDetail()));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
    String givenEdifact = UUID.randomUUID().toString();
    EventContext eventContext = new EventContext();
    eventContext.putObject(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    eventContext.putObject(EDIFACT.value(), givenEdifact);
    eventContext.setCurrentNode(matchProfileWrapper);
    // when
    boolean result = MatchingManager.match(eventContext);
    // then
    assertTrue(result);
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowException_ifNoEligibleReader() {
    // given
    MatchValueLoaderFactory.register(new TestMatchValueLoader());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withIncomingRecordType(EDIFACT)
      .withMatchDetails(Collections.singletonList(new MatchDetail()));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    EventContext eventContext = new EventContext();
    eventContext.setCurrentNode(matchProfileWrapper);
    // when
    MatchingManager.match(eventContext);
    // then expect runtime exception
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowException_ifNoEligibleLoader() {
    // given
    MatchValueReaderFactory.register(new TestMatchValueReader());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withIncomingRecordType(EDIFACT)
      .withMatchDetails(Collections.singletonList(new MatchDetail()));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    EventContext eventContext = new EventContext();
    eventContext.setCurrentNode(matchProfileWrapper);

    // when
    MatchingManager.match(eventContext);
    // then expect runtime exception
  }
}
