package org.folio.processing.matching;

import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;

import java.util.Map;

import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
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

import io.netty.util.internal.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Matching Manager implementation, provides ability to perform matching
 */
public final class MatchingManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MatchingManager.class);
  private static final String MAPPING_PARAMS = "MAPPING_PARAMS";
  private static final String RELATIONS = "MATCHING_PARAMETERS_RELATIONS";

  private MatchingManager() {
  }

  public static boolean match(DataImportEventPayload eventPayload) {
    try {
      if (eventPayload.getCurrentNode().getContentType() != MATCH_PROFILE) {
        LOGGER.info("Current node is not of {} content type", MATCH_PROFILE);
        return false;
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
      return new Matcher() {
      }.match(reader, loader, eventPayload);
    } catch (Exception e) {
      throw new MatchingException(e);
    }
  }

  public static String retrieveIdFromContext(MatchDetail matchDetail, DataImportEventPayload eventPayload) {
    JsonObject matchingParams = new JsonObject(eventPayload.getContext().get(MAPPING_PARAMS));
    JsonObject relations = new JsonObject(eventPayload.getContext().get(RELATIONS));
    String relation = String.valueOf(relations.getJsonObject("matchingRelations")
      .getMap().get(matchDetail.getExistingMatchExpression().getFields().get(0).getValue()));
    JsonArray jsonArray = matchingParams.getJsonArray(relation);

    for (int i = 0; i < jsonArray.size(); i++) {
      if (jsonArray.getJsonObject(i).getString("name")
        .equals(matchDetail.getIncomingMatchExpression().getStaticValueDetails().getText().trim())) {
        JsonObject result = jsonArray.getJsonObject(i);
        return result.getString("id");
      }
    }
    return StringUtil.EMPTY_STRING;
  }
}
