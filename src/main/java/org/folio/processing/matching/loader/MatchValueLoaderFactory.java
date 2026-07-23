package org.folio.processing.matching.loader;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import org.folio.rest.jaxrs.model.EntityType;

public class MatchValueLoaderFactory {

  private static final List<MatchValueLoader> matchValueLoaderList = new ArrayList<>();

  private MatchValueLoaderFactory() { }

  public static MatchValueLoader build(EntityType existingRecordType) {
    return matchValueLoaderList.stream()
      .filter(matchValueLoader -> matchValueLoader.isEligibleForEntityType(existingRecordType))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException(
        format("Can not find MatchValueLoader by entity type [%s]", existingRecordType)));
  }

  public static void register(MatchValueLoader matchValueLoader) {
    matchValueLoaderList.add(matchValueLoader);
  }

  public static void clearLoaderFactory() {
    matchValueLoaderList.clear();
  }
}
