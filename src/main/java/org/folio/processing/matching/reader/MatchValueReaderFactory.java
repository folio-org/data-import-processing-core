package org.folio.processing.matching.reader;

import org.folio.rest.jaxrs.model.EntityType;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class MatchValueReaderFactory {
  private static final List<MatchValueReader> matchValueReaderList = new ArrayList<>();

  public static MatchValueReader build(EntityType incomingRecordType) {
    return matchValueReaderList.stream()
      .filter(matchValueReader -> matchValueReader.isEligibleForEntityType(incomingRecordType))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException(format("Can not find MatchValueReader by entity type [%s]", incomingRecordType)));
  }

  public static void register(MatchValueReader matchValueReader) {
    matchValueReaderList.add(matchValueReader);
  }

  public static void clearReaderFactory() {
    matchValueReaderList.clear();
  }
}
