package org.folio.processing.mapping.mapper.writer;

import org.folio.processing.mapping.model.MappingProfile.EntityType;

/**
 * Factory to produce readers
 *
 * @see Writer
 * @see EntityType
 */
public interface WriterFactory {

  /**
   * Creates a writer
   *
   * @return writer
   */
  Writer createWriter();

  /**
   * Checks if factory can produce entity of given type
   *
   * @param entityType type of entity
   * @return true if given type of entity is eligible
   */
  boolean isEligibleForEntityType(EntityType entityType);
}
