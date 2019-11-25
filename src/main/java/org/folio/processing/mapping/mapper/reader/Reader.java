package org.folio.processing.mapping.mapper.reader;

import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.mapper.value.Value;
import org.folio.processing.mapping.model.Rule;

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
  void initialize(EventContext eventContext);

  /**
   * Reads value from the underlying entity using mapping rule.
   *
   * @param rule mapping rule
   * @return Value value
   * @see Value
   * @see Rule
   */
  Value read(Rule rule);
}
