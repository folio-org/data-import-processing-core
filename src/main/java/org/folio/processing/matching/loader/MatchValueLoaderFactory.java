package org.folio.processing.matching.loader;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import org.folio.rest.jaxrs.model.EntityType;

public final class MatchValueLoaderFactory {

  private static final List<MatchValueLoader> MATCH_VALUE_LOADERS = new ArrayList<>();

  private MatchValueLoaderFactory() { }

  public static MatchValueLoader build(EntityType existingRecordType) {
    return MATCH_VALUE_LOADERS.stream()
      .filter(matchValueLoader -> matchValueLoader.isEligibleForEntityType(existingRecordType))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException(
        format("Can not find MatchValueLoader by entity type [%s]", existingRecordType)));
  }

  public static void register(MatchValueLoader matchValueLoader) {
    MATCH_VALUE_LOADERS.add(matchValueLoader);
  }

  public static void clearLoaderFactory() {
    MATCH_VALUE_LOADERS.clear();
  }
}
