package org.folio.processing.mapping.mapper.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.folio.DataImportEventPayload;
import org.folio.processing.value.Value;

import java.io.IOException;

/**
 * The root interface for Writers.
 * The purpose of Writer is to write a given Value to underlying entity by the given fieldPath.
 * Writer has to be initialized before write.
 *
 * @see Value
 */
public interface Writer {

  /**
   * Performs initialization of the writer using event payload.
   *
   * @param eventPayload event payload
   * @throws IOException if a low-level I/O problem occurs (JSON serialization)
   */
  void initialize(DataImportEventPayload eventPayload) throws IOException;

  /**
   * Writes value to the underlying entity by the fieldPath
   *
   * @param fieldPath path to the certain field of the entity
   * @param value     value
   * @see Value
   */
  void write(String fieldPath, Value value);

  /**
   * Puts result of writing into event payload and returns event payload
   *
   * @param eventPayload event payload
   * @return event payload
   * @throws JsonProcessingException if json serialization problem occurred
   */
  DataImportEventPayload getResult(DataImportEventPayload eventPayload) throws JsonProcessingException;
}
