package org.folio.processing.matching.reader.util;

import org.apache.commons.lang.StringUtils;
import org.folio.processing.matching.model.schemas.Qualifier;

import static org.folio.processing.matching.model.schemas.Qualifier.ComparisonPart.ALPHANUMERICS_ONLY;
import static org.folio.processing.matching.model.schemas.Qualifier.ComparisonPart.NUMERICS_ONLY;
import static org.folio.processing.matching.model.schemas.Qualifier.QualifierType.BEGINS_WITH;
import static org.folio.processing.matching.model.schemas.Qualifier.QualifierType.CONTAINS;
import static org.folio.processing.matching.model.schemas.Qualifier.QualifierType.ENDS_WITH;

/**
 * Util class to process value according to MatchExpression
 */
public final class MatchExpressionUtil {

  private static final String NON_DIGIT = "\\D+";
  private static final String NON_DIGIT_AND_NON_ALPHA = "[^\\p{L}\\p{N}]";

  private MatchExpressionUtil() {}

  /**
   * Extracts specified comparison part of the value
   *
   * @param value original value
   * @param qualifier qualifier specifying which comparison part should be extracted from the value
   * @return comparison part of the value
   */
  public static String extractComparisonPart(String value, Qualifier qualifier) {
    if (value != null) {
      if (qualifier == null || qualifier.getComparisonPart() == null) {
        return value;
      } else if (qualifier.getComparisonPart() == NUMERICS_ONLY) {
        return value.replaceAll(NON_DIGIT, StringUtils.EMPTY);
      } else if (qualifier.getComparisonPart() == ALPHANUMERICS_ONLY) {
        return value.replaceAll(NON_DIGIT_AND_NON_ALPHA, StringUtils.EMPTY);
      }
    }
    return value;
  }

  /**
   * Checks whether value is qualified to be compared in matching process
   *
   * @param value original value
   * @param qualifier qualifier specifying conditions that value should satisfy in order to be used for matching purposes
   * @return true if value is qualified for matching
   */
  public static boolean isQualified(String value, Qualifier qualifier) {
    if (value != null) {
      if (qualifier == null || qualifier.getQualifierType() == null) {
        return true;
      } else if (qualifier.getQualifierType() == BEGINS_WITH) {
        return value.startsWith(qualifier.getQualifierValue());
      } else if (qualifier.getQualifierType() == ENDS_WITH) {
        return value.endsWith(qualifier.getQualifierValue());
      } else if (qualifier.getQualifierType() == CONTAINS) {
        return value.contains(qualifier.getQualifierValue());
      }
    }
     return false;
  }

}
