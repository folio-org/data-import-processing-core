package org.folio.processing.mapping.mapper.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.mapper.value.Value;

import java.io.IOException;

/**
 * The root interface for writers.
 * The purpose of Writer is to write a given Value to underlying entity by the given fieldPath.
 * Writer has to be initialized before write.
 *
 * @see Value
 */
public interface Writer {

    /**
     * Performs initialization of the writer using event context.
     *
     * @param eventContext event context
     * @throws IOException if a low-level I/O problem occurs (JSON serialization)
     */
    void initialize(EventContext eventContext) throws IOException;

    /**
     * Writes value to the underlying entity by the fieldPath
     *
     * @param fieldPath path to the certain field of the entity
     * @param value     value
     * @see Value
     */
    void write(String fieldPath, Value value);

  /**
   * TODO javadoc
   * @param eventContext
   * @return
   * @throws JsonProcessingException
   */
    EventContext getResult(EventContext eventContext) throws JsonProcessingException;
}
