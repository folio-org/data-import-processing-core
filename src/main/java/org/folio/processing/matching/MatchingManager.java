package org.folio.processing.matching;

import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;

import java.util.Map;

import org.folio.DataImportEventPayload;
import org.folio.MatchProfile;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.matcher.Matcher;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import io.vertx.core.json.JsonObject;

/**
 * Matching Manager implementation, provides ability to perform matching
 */
public final class MatchingManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MatchingManager.class);

  private MatchingManager() {
  }

  public static CompletableFuture<Boolean> match(DataImportEventPayload eventPayload) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    try {
      if (eventPayload.getCurrentNode().getContentType() != MATCH_PROFILE) {
        LOGGER.info("Current node is not of {} content type", MATCH_PROFILE);
        future.complete(false);
        return future;
      }
      ProfileSnapshotWrapper matchingProfileWrapper = eventPayload.getCurrentNode();
      MatchProfile matchProfile;
      if (matchingProfileWrapper.getContent() instanceof Map) {
        matchProfile = new JsonObject((Map) matchingProfileWrapper.getContent()).mapTo(MatchProfile.class);
      } else {
        matchProfile = (MatchProfile) matchingProfileWrapper.getContent();
      }
      MatchValueReader reader = MatchValueReaderFactory.build(matchProfile.getIncomingRecordType());
      MatchValueLoader loader = MatchValueLoaderFactory.build(matchProfile.getExistingRecordType());
      future = new Matcher() {
      }.match(reader, loader, eventPayload);
    } catch (Exception e) {
      future.completeExceptionally(new MatchingException(e));
    }
    return future;
  }
}
