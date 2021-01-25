package org.folio.processing.mapping.defaultmapper.processor.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.BooleanUtils;
import org.marc4j.marc.DataField;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Util for processing fields with specific logic.
 */
public final class ExtraFieldUtil {

  public static final String FIELD_REPLACEMENT_BY_3_DIGITS_PROPERTY = "fieldReplacementBy3Digits";
  public static final String FIELD_REPLACEMENT_RULE_PROPERTY = "fieldReplacementRule";
  public static final String SUBFIELD_PROPERTY = "subfield";
  public static final String SOURCE_DIGITS_PROPERTY = "sourceDigits";
  public static final String TARGET_FIELD_PROPERTY = "targetField";

  private ExtraFieldUtil() {
  }

  /**
   * Finds 'fieldReplacementBy3Digits'(or other fieldReplacement rule) field, with 'true' value. If exists, retrieves 'fieldReplacementRule', which
   * contains 'sourceDigits' and 'targetField' field, which contains matching between first 3 (for example)
   * digits (from specific subfield from rules) and target field for this value which should be processed. After that, change source field on 'targetField'.
   * If not matches, change just on first 3 digits value.
   * More info: https://issues.folio.org/browse/MODDICORE-114
   *
   * @param field - field from record, which will be changed if need
   * @param mappingRules - rules for default mapping.
   */
  public static void findAndReplaceFieldsIfNeed(DataField field, JsonObject mappingRules) {
    JsonArray mappingEntry = mappingRules.getJsonArray(field.getTag());
    if (mappingEntry == null) {
      return;
    }

    for (int i = 0; i < mappingEntry.size(); i++) {
      JsonObject subFieldMapping = mappingEntry.getJsonObject(i);
      boolean fieldReplacementBy3Digits = BooleanUtils.isTrue(subFieldMapping.getBoolean(
        FIELD_REPLACEMENT_BY_3_DIGITS_PROPERTY));

      if (fieldReplacementBy3Digits) {
        processReplacementBasedOn3Digits(field, subFieldMapping);
      }
    }
  }

  private static void processReplacementBasedOn3Digits(DataField field, JsonObject subFieldMapping) {
    JsonArray fieldReplacementRules = subFieldMapping.getJsonArray(FIELD_REPLACEMENT_RULE_PROPERTY);
    Map<String, String> replacementRules =  retrieveReplacementRules(fieldReplacementRules);
    JsonArray subfields = subFieldMapping.getJsonArray(SUBFIELD_PROPERTY);
    for (Object subfield : subfields) {
      String data = field.getSubfield(String.valueOf(subfield).charAt(0)).getData();
      String targetField;
      String first3Digits = data.substring(0, 3);
      targetField = replacementRules.getOrDefault(first3Digits, first3Digits);
      field.setTag(targetField);
    }
  }

  private static Map<String, String> retrieveReplacementRules(JsonArray fieldReplacementRules) {
    Map<String, String> replacementRules = new HashMap<>();
    for (int i = 0; i < fieldReplacementRules.size(); i++) {
      replacementRules.put(fieldReplacementRules.getJsonObject(i).getString(SOURCE_DIGITS_PROPERTY),
        fieldReplacementRules.getJsonObject(i).getString(TARGET_FIELD_PROPERTY));
    }
    return replacementRules;
  }
}
