package org.folio.processing.matching.matcher;

import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.rest.jaxrs.model.EntityType;

public interface MatcherFactory {
  Matcher createMatcher(MatchValueReader matchValueReader, MatchValueLoader matchValueLoader);

  boolean isEligibleForEntityType(EntityType entityType);
}
