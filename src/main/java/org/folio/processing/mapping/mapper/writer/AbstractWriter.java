package org.folio.processing.mapping.mapper.writer;

import org.folio.processing.value.BooleanValue;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MapValue;
import org.folio.processing.value.RepeatableFieldValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;

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
      case MAP:
        writeObjectValue(fieldPath, (MapValue) value);
        break;
      case REPEATABLE:
        writeRepeatableValue(fieldPath, (RepeatableFieldValue) value);
        break;
      case BOOLEAN:
        writeBooleanValue(fieldPath, (BooleanValue) value);
        break;
      case MISSING:
        break;
      default:
        throw new IllegalArgumentException("Can not define value type");
    }
  }

  protected abstract void writeStringValue(String fieldPath, StringValue value);

  protected abstract void writeListValue(String fieldPath, ListValue value);

  protected abstract void writeObjectValue(String fieldPath, MapValue value);

  protected abstract void writeRepeatableValue(String fieldPath, RepeatableFieldValue value);

  protected abstract void writeBooleanValue(String fieldPath, BooleanValue value);
}
