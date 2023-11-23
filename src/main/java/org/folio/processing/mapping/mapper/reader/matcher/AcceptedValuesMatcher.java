package org.folio.processing.mapping.mapper.reader.matcher;

/**
 * A comparison function, which determines whether the specified value
 * matches to accepted value retrieved from a {@link org.folio.MappingProfile}
 */
@FunctionalInterface
public interface AcceptedValuesMatcher {

  /**
   * Checks whether specified {@code valueToCompare} matches to the {@code acceptedValue}
   * by criteria that depend on this method implementation.
   *
   * @param acceptedValue   - accepted value from the mapping profile
   * @param valueToCompare  - value to compare to the accepted value
   * @return true if the {@code valueToCompare} matches to the {@code acceptedValue}, otherwise false
   */
  boolean matches(String acceptedValue, String valueToCompare);

}
