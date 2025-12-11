package org.folio.processing.matching;

import static org.folio.processing.events.utils.EventUtils.extractRecordId;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;

import java.util.Map;

import org.folio.DataImportEventPayload;
import org.folio.MatchProfile;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.mapping.mapper.FactoryRegistry;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.matcher.Matcher;
import org.folio.processing.matching.matcher.MatcherFactory;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;

import io.vertx.core.json.JsonObject;

/**
 * Matching Manager implementation, provides ability to perform matching
 */
public final class MatchingManager {
  private static final Logger LOGGER = LogManager.getLogger(MatchingManager.class);
  private static final FactoryRegistry FACTORY_REGISTRY = new FactoryRegistry();

  private MatchingManager() {
  }

  public static CompletableFuture<Boolean> match(DataImportEventPayload eventPayload) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    try {
      if (eventPayload.getCurrentNode().getContentType() != MATCH_PROFILE) {
        LOGGER.info("match:: Current node is not of {} content type jobExecutionId: {} recordId: {}",
          MATCH_PROFILE, eventPayload.getJobExecutionId(), extractRecordId(eventPayload));
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
      Matcher matcher = FACTORY_REGISTRY.createMatcher(matchProfile.getExistingRecordType(), reader, loader);

      return matcher.match(eventPayload);
    } catch (Exception e) {
      LOGGER.warn("match:: Failed to perform matching jobExecutionId: {} recordId: {}",
        eventPayload.getJobExecutionId(), extractRecordId(eventPayload), e);
      future.completeExceptionally(new MatchingException(e));
    }
    return future;
  }

  public static boolean registerMatcherFactory(MatcherFactory matcherFactory) {
    return FACTORY_REGISTRY.getMatcherFactories().add(matcherFactory);
  }

  public static void clearMatcherFactories() {
    FACTORY_REGISTRY.getMatcherFactories().clear();
  }
}
