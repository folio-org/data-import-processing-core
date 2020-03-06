package org.folio.processing.matching.manager;

import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.processing.matching.MatchingManager;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class MatchingManagerTest {

  @Before
  public void beforeTest() {
    MatchValueReaderFactory.clearReaderFactory();
    MatchValueLoaderFactory.clearLoaderFactory();
  }

  @Test
  public void shouldMatch_MarcBibliographicAndEdifact() {
    // given
    MatchValueReaderFactory.register(new TestMatchValueReader());
    MatchValueLoaderFactory.register(new TestMatchValueLoader());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withIncomingRecordType(EDIFACT_INVOICE)
      .withMatchDetails(Collections.singletonList(new MatchDetail()));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
    String givenEdifact = UUID.randomUUID().toString();
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(EDIFACT_INVOICE.value(), givenEdifact);
    eventContext.setContext(context);
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
      .withIncomingRecordType(EDIFACT_INVOICE)
      .withMatchDetails(Collections.singletonList(new MatchDetail()));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    DataImportEventPayload eventContext = new DataImportEventPayload();
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
      .withIncomingRecordType(EDIFACT_INVOICE)
      .withMatchDetails(Collections.singletonList(new MatchDetail()));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    DataImportEventPayload eventContext = new DataImportEventPayload();
    eventContext.setCurrentNode(matchProfileWrapper);

    // when
    MatchingManager.match(eventContext);
    // then expect runtime exception
  }

  @Test
  public void shouldNotMatchIfWrongContentType() {
    // given
    MatchValueReaderFactory.register(new TestMatchValueReader());
    MatchValueLoaderFactory.register(new TestMatchValueLoader());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withIncomingRecordType(EDIFACT_INVOICE)
      .withMatchDetails(Collections.singletonList(new MatchDetail()));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
    String givenEdifact = UUID.randomUUID().toString();
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(EDIFACT_INVOICE.value(), givenEdifact);
    eventContext.setContext(context);
    eventContext.setCurrentNode(matchProfileWrapper);
    // when
    boolean result = MatchingManager.match(eventContext);
    // then
    assertFalse(result);
  }
}
