package org.folio.processing.mapping.mapper.reader;

import static org.folio.processing.mapping.model.MappingProfile.EntityType;

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
