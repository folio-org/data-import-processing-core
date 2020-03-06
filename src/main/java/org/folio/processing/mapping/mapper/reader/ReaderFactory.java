package org.folio.processing.mapping.mapper.reader;

import org.folio.rest.jaxrs.model.EntityType;

/**
 * Factory to produce readers
 *
 * @see Reader
 * @see EntityType
 */
public interface ReaderFactory {

  /**
   * Creates a reader
   *
   * @return reader
   */
  Reader createReader();

  /**
   * Checks if factory can produce entity of given type
   *
   * @param entityType type of entity
   * @return true if given type of entity is eligible
   */
  boolean isEligibleForEntityType(EntityType entityType);
}
