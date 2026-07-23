package org.folio.processing.matching.reader;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import org.folio.rest.jaxrs.model.EntityType;

public class MatchValueReaderFactory {
  private static final List<MatchValueReader> MATCH_VALUE_READERS = new ArrayList<>();

  public static MatchValueReader build(EntityType incomingRecordType) {
    return MATCH_VALUE_READERS.stream()
      .filter(matchValueReader -> matchValueReader.isEligibleForEntityType(incomingRecordType))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException(
        format("Can not find MatchValueReader by entity type [%s]", incomingRecordType)));
  }

  public static void register(MatchValueReader matchValueReader) {
    MATCH_VALUE_READERS.add(matchValueReader);
  }

  public static void clearReaderFactory() {
    MATCH_VALUE_READERS.clear();
  }
}
