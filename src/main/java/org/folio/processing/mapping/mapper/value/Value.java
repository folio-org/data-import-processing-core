package org.folio.processing.mapping.mapper.value;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;

/**
 * Generic wrapper for various values.
 * Represents value that reader reads and passes to writer.
 *
 * @param <T> type of value
 * @see ValueType
 * @see Reader
 * @see Writer
 */
public interface Value<T> {

    /**
     * Returns value
     *
     * @return value
     */
    T getValue();

    /**
     * Returns type of the underlying value
     *
     * @return type of value
     */
    ValueType getType();

    /**
     * Enumeration to describe type of generic value
     */
    enum ValueType {
        STRING,
        LIST
    }
}
