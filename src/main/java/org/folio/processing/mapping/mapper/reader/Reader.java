package org.folio.processing.mapping.mapper.reader;

import org.folio.DataImportEventPayload;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.MappingRule;

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
   * Performs initialization of the reader using event payload.
   *
   * @param eventPayload event payload
   */
  void initialize(DataImportEventPayload eventPayload) throws IOException;

  /**
   * Reads value from the underlying entity using mapping rule.
   *
   * @param ruleExpression rule expression defines an address of source to read value from
   * @return Value value
   * @see Value
   * @see MappingRule
   */
  Value read(MappingRule ruleExpression);
}
