package org.folio.processing.matching.loader;

import org.folio.processing.matching.model.schemas.MatchProfile;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class MatchValueLoaderFactory {

  private MatchValueLoaderFactory() {}

  private static final List<MatchValueLoader> matchValueLoaderList = new ArrayList<>();

  public static MatchValueLoader build(MatchProfile.ExistingRecordType existingRecordType) {
    return matchValueLoaderList.stream()
      .filter(matchValueLoader -> matchValueLoader.isEligibleForEntityType(existingRecordType))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException(format("Can not find MatchValueLoader by entity type [%s]", existingRecordType)));
  }

  public static void register(MatchValueLoader matchValueLoader) {
    matchValueLoaderList.add(matchValueLoader);
  }

  public static void clearLoaderFactory() {
    matchValueLoaderList.clear();
  }
}
