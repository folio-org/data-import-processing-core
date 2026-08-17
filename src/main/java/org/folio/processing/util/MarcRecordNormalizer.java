package org.folio.processing.util;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.VariableField;

public final class MarcRecordNormalizer {

  private static final String TAG_035 = "035";
  private static final String OCLC_PREFIX = "(OCoLC)";
  private static final String OCLC = "OCoLC";
  private static final String OCLC_PATTERN = "\\((" + OCLC + ")\\)((ocm|ocn|on)?0*|([a-zA-Z]+)0*)(\\d+\\w*)";
  private static final Pattern OCLC_COMPILED = Pattern.compile(OCLC_PATTERN);
  private static final Pattern DOT_OR_WHITESPACE_PATTERN = Pattern.compile("[.\\s]");
  private static final Pattern DIGITS_PATTERN = Pattern.compile("\\d+");
  private static final Pattern PREFIX_ZEROS_PATTERN = Pattern.compile("^0+");

  private MarcRecordNormalizer() {
  }

  public static void normalize035Field(Record marcRecord) {
    if (marcRecord == null) {
      return;
    }
    var subfields = get035SubfieldOclcValues(marcRecord);
    if (!subfields.isEmpty()) {
      formatOclc(subfields);
      deduplicateOclc(marcRecord, subfields);
    }
  }

  private static List<Subfield> get035SubfieldOclcValues(Record marcRecord) {
    List<Subfield> subfields = new ArrayList<>();
    for (VariableField field : marcRecord.getVariableFields(TAG_035)) {
      if (field instanceof DataField dataField) {
        for (Subfield sf : dataField.getSubfields()) {
          if (sf.getData() != null && sf.getData().trim().startsWith(OCLC_PREFIX)) {
            subfields.add(sf);
          }
        }
      }
    }
    return subfields;
  }

  private static void formatOclc(List<Subfield> subfields) {
    for (Subfield subfield : subfields) {
      subfield.setData(normalizeOclcValue(subfield.getData()));
    }
  }

  public static String normalizeOclcValue(String data) {
    data = DOT_OR_WHITESPACE_PATTERN.matcher(data).replaceAll("");
    var matcher = OCLC_COMPILED.matcher(data);
    if (!matcher.find()) {
      return data;
    }
    var oclcTag = matcher.group(1); // "OCoLC"
    var numericAndTrailing = matcher.group(5);  // Numeric part and any characters that follow
    var prefix = matcher.group(2); // Entire prefix including letters and potentially leading zeros

    if (prefix != null && (prefix.startsWith("ocm") || prefix.startsWith("ocn") || prefix.startsWith("on"))) {
      // If "ocm" or "ocn", strip entirely from the prefix
      return "(" + oclcTag + ")" + numericAndTrailing;
    }
    // For other cases, strip leading zeros only from the numeric part
    numericAndTrailing = PREFIX_ZEROS_PATTERN.matcher(numericAndTrailing).replaceFirst("");
    if (prefix != null) {
      prefix = DIGITS_PATTERN.matcher(prefix).replaceAll(""); // Remove digits from the prefix if not null
    }
    // Add back any other prefix that might have been included like "tfe"
    return "(" + oclcTag + ")" + (prefix != null ? prefix : "") + numericAndTrailing;
  }

  private static void deduplicateOclc(Record marcRecord, List<Subfield> subfields) {
    var parentMap = new IdentityHashMap<Subfield, DataField>();
    marcRecord.getVariableFields(TAG_035).forEach(vf -> {
      if (vf instanceof DataField df) {
        df.getSubfields().forEach(sf -> parentMap.put(sf, df));
      }
    });

    var subfieldsToDelete = new ArrayList<Subfield>();
    for (var subfield : new ArrayList<>(subfields)) {
      var duplicate = subfields.stream().filter(s -> isDuplicate(subfield, s)).findFirst();
      if (duplicate.isPresent()) {
        // Prefer to keep the subfield in the field that would be orphaned without it
        // (sole subfield of its code in a multi-subfield field), e.g. $a in a 035 that also has $z.
        boolean preferKeepCurrent = isSoleOfCodeInMultiSubfieldField(subfield, parentMap)
          && !isSoleOfCodeInMultiSubfieldField(duplicate.get(), parentMap);
        subfieldsToDelete.add(preferKeepCurrent ? duplicate.get() : subfield);
        subfields.remove(subfield);
      }
    }
    var variableFields = marcRecord.getVariableFields(TAG_035);
    subfieldsToDelete.forEach(subfieldToDelete ->
      variableFields.forEach(field -> removeSubfieldIfExist(marcRecord, field, subfieldToDelete)));
  }

  private static boolean isDuplicate(Subfield s1, Subfield s2) {
    return s1 != s2
           && s1.getData().equals(s2.getData())
           && s1.getCode() == s2.getCode();
  }

  /**
   * Returns true when {@code subfield} is the only subfield of its code in its parent DataField
   * and the parent has other subfields of different codes. Removing such a subfield would leave
   * the parent without its primary marker (e.g. $a) while other subfields ($z, $b, …) remain,
   * which creates an incomplete 035 field.
   */
  private static boolean isSoleOfCodeInMultiSubfieldField(Subfield subfield,
                                                           Map<Subfield, DataField> parentMap) {
    var parent = parentMap.get(subfield);
    return parent != null
      && parent.getSubfields(subfield.getCode()).size() == 1
      && parent.getSubfields().size() > 1;
  }

  private static void removeSubfieldIfExist(Record marcRecord, VariableField field,
                                            Subfield subfieldToDelete) {
    if (field instanceof DataField dataField && dataField.getSubfields().contains(subfieldToDelete)) {
      if (dataField.getSubfields().size() > 1) {
        dataField.removeSubfield(subfieldToDelete);
      } else {
        marcRecord.removeVariableField(dataField);
      }
    }
  }
}
