package org.folio.processing.matching.loader.query;

import org.folio.processing.matching.model.schemas.MatchDetail;
import org.folio.processing.value.Value;

public class LoadQueryBuilder {

  private LoadQueryBuilder() {}

  public static LoadQuery build(Value value, MatchDetail matchDetail) {
    return new DefaultLoadQuery();
  }
}
