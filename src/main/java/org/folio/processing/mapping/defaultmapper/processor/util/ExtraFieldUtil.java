package org.folio.processing.mapping.defaultmapper.processor.util;

import org.marc4j.marc.DataField;
import org.marc4j.marc.Record;

public final class ExtraFieldUtil {

  public static final String FIELD_880 = "880";
  public static final String FIELD_100 = "100";
  public static final String FIELD_110 = "110";
  public static final String FIELD_111 = "111";
  public static final String FIELD_245 = "245";
  public static final String FIELD_246 = "246";
  public static final String FIELD_700 = "700";
  public static final String FIELD_710 = "710";
  public static final String FIELD_711 = "711";
  public static final char SUBFIELD_6 = '6';

  private ExtraFieldUtil() {
  }

  public static void findAndModify880FieldIfExists(Record record) {
    for (DataField field : record.getDataFields()) {
      if (field.getTag().equals(FIELD_880)) {
        String data = field.getSubfield(SUBFIELD_6).getData();
        String targetField;
        String first3Digits = data.substring(0, 3);
        switch (first3Digits) {
          case (FIELD_100):
            targetField = FIELD_700;
            break;
          case (FIELD_110):
            targetField = FIELD_710;
            break;
          case (FIELD_111):
            targetField = FIELD_711;
            break;
          case (FIELD_245):
            targetField = FIELD_246;
            break;
          default:
            targetField = first3Digits;
        }
        field.setTag(targetField);
      }
    }
  }
}
