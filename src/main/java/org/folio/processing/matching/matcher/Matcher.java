package org.folio.processing.matching.matcher;

import java.util.concurrent.CompletableFuture;
import org.folio.DataImportEventPayload;

public interface Matcher {
  CompletableFuture<Boolean> match(DataImportEventPayload eventPayload);
}
