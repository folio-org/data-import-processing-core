package org.folio.processing.matching;

import org.folio.DataImportEventPayload;
import org.folio.MatchProfile;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.matcher.Matcher;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.matching.reader.MatchValueReaderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;

/**
 * Matching Manager implementation, provides ability to perform matching
 */
public final class MatchingManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MatchingManager.class);

  private MatchingManager() {
  }

  public static boolean match(DataImportEventPayload eventPayload) {
    try {
      if (eventPayload.getCurrentNode().getContentType() != MATCH_PROFILE) {
        LOGGER.info("Current node is not of {} content type", MATCH_PROFILE);
        return false;
      }
      ProfileSnapshotWrapper matchingProfileWrapper = eventPayload.getCurrentNode();
      MatchProfile matchProfile = (MatchProfile) matchingProfileWrapper.getContent();
      MatchValueReader reader = MatchValueReaderFactory.build(matchProfile.getIncomingRecordType());
      MatchValueLoader loader = MatchValueLoaderFactory.build(matchProfile.getExistingRecordType());
      return new Matcher() {}.match(reader, loader, eventPayload);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
