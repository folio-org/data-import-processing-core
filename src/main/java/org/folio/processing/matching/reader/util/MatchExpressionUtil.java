package org.folio.processing.matching.reader.util;

import static org.folio.rest.jaxrs.model.Qualifier.ComparisonPart.ALPHANUMERICS_ONLY;
import static org.folio.rest.jaxrs.model.Qualifier.ComparisonPart.NUMERICS_ONLY;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.Qualifier;

/**
 * Util class to process value according to MatchExpression.
 */
public final class MatchExpressionUtil {

  // Both patterns mirror the Postgres classes the storage side normalizes with: digit is ASCII-only,
  // and alnum on a UTF-8 ctype covers Nd and Nl but not No (fractions, superscripts, circled numerals).
  private static final String NON_DIGIT = "[^0-9]";
  private static final String NON_DIGIT_AND_NON_ALPHA = "[^\\p{L}\\p{Nd}\\p{Nl}]";

  private MatchExpressionUtil() {
  }

  /**
   * Extracts specified comparison part of the value.
   *
   * @param value     original value
   * @param qualifier qualifier specifying which comparison part should be extracted from the value
   * @return comparison part of the value
   */
  public static String extractComparisonPart(String value, Qualifier qualifier) {
    if (value != null && qualifier != null && qualifier.getComparisonPart() != null) {
      if (qualifier.getComparisonPart() == NUMERICS_ONLY) {
        return value.replaceAll(NON_DIGIT, StringUtils.EMPTY);
      } else if (qualifier.getComparisonPart() == ALPHANUMERICS_ONLY) {
        return value.replaceAll(NON_DIGIT_AND_NON_ALPHA, StringUtils.EMPTY);
      }
    }
    return value;
  }

  /**
   * Checks whether value is qualified to be compared in matching process.
   *
   * <p>A qualifier that has no qualifier value applies no filtering, since there is nothing to compare the value
   * against
   *
   * @param value     original value
   * @param qualifier qualifier specifying conditions that value should satisfy to be used for matching purposes
   * @return true if value is qualified for matching
   */
  public static boolean isQualified(String value, Qualifier qualifier) {
    boolean isQualified = true;
    if (value != null && qualifier != null && qualifier.getQualifierType() != null
        && StringUtils.isNotEmpty(qualifier.getQualifierValue())) {
      isQualified = switch (qualifier.getQualifierType()) {
        case BEGINS_WITH -> value.startsWith(qualifier.getQualifierValue());
        case ENDS_WITH -> value.endsWith(qualifier.getQualifierValue());
        case CONTAINS -> value.contains(qualifier.getQualifierValue());
        default -> false;
      };
    }
    return isQualified;
  }
}
