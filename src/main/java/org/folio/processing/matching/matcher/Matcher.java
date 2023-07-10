package org.folio.processing.matching.matcher;

import org.folio.DataImportEventPayload;
import java.util.concurrent.CompletableFuture;

public interface Matcher {
  CompletableFuture<Boolean> match(DataImportEventPayload eventPayload);
}
