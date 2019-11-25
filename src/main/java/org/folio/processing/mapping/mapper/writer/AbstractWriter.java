package org.folio.processing.mapping.mapper.writer;

import org.folio.processing.mapping.mapper.value.ListValue;
import org.folio.processing.mapping.mapper.value.StringValue;
import org.folio.processing.mapping.mapper.value.Value;


/**
 * Abstract class for writers
 */
public abstract class AbstractWriter implements Writer {

    @Override
    public void write(String fieldPath, Value value) {
        switch (value.getType()) {
            case STRING:
                writeStringValue(fieldPath, (StringValue) value);
                break;
            case LIST:
                writeListValue(fieldPath, (ListValue) value);
                break;
            default:
                throw new IllegalArgumentException("Can not define value type");
        }
    }

    protected abstract void writeStringValue(String fieldPath, StringValue value);

    protected abstract void writeListValue(String fieldPath, ListValue value);
}
