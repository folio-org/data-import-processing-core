package org.folio.processing.mapping.defaultmapper;

import java.util.List;

/**
 * Builder for creating a mapper object by type of the records
 */
public final class RecordMapperBuilder {

  @SuppressWarnings("rawtypes")
  private static final List<RecordMapper> mappers = List.of(new MarcToInstanceMapper(), new MarcToHoldingsMapper(), new MarcToAuthorityMapper());

  private RecordMapperBuilder() {
  }

  /**
   * Builds specific mapper based on the record format
   *
   * @param format - record format
   * @return - RecordToInstanceMapper for the specified record format
   */
  @SuppressWarnings("unchecked")
  public static <T> RecordMapper<T> buildMapper(String format) {
    return mappers.stream()
      .filter(mapper -> mapper.getMapperFormat().equals(format))
      .findFirst()
      .orElseThrow(() -> new RecordToInstanceMapperNotFoundException(String.format("Record to Instance Mapper was not found for Record Format: %s", format)));
  }
}
