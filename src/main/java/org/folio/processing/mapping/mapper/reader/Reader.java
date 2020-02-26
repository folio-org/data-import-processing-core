package org.folio.processing.mapping.mapper.reader;

import org.folio.DataImportEventPayload;
import org.folio.processing.value.Value;
import org.folio.processing.mapping.model.Rule;

import java.io.IOException;

/**
 * The root interface for Readers.
 * The purpose of Reader is to read Value by rule from underlying entity.
 * Reader has to be initialized before read.
 *
 * @see Value
 */
public interface Reader {

  /**
   * Performs initialization of the reader using event context.
   *
   * @param eventContext event context
   */
  void initialize(DataImportEventPayload eventContext) throws IOException;

  /**
   * Reads value from the underlying entity using mapping rule.
   *
   * @param ruleExpression rule expression defines an address of source to read value from
   * @return Value value
   * @see Value
   * @see Rule
   */
  Value read(String ruleExpression);
}
