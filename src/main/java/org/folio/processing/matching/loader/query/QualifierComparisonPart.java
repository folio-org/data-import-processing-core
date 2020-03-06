package org.folio.processing.matching.loader.query;

import org.folio.rest.jaxrs.model.Qualifier.ComparisonPart;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Allows to apply comparison part to sql and cql queries according to the {@link ComparisonPart},
 * FIELD_NAME token is used to mark the place for field reference
 */
public enum QualifierComparisonPart {

  // TODO provide expressions for cql queries
  NUMERICS_ONLY("REGEXP_REPLACE(FIELD_NAME, '[^[:digit:]]','','g')",
    "FIELD_NAME"),
  ALPHANUMERICS_ONLY("REGEXP_REPLACE(FIELD_NAME, '[^[:alnum:]]','','g')",
    "FIELD_NAME");

  private String sqlSubstitute;
  private String cqlSubstitute;

  QualifierComparisonPart(String sqlSubstitute, String cqlSubstitute) {
    this.sqlSubstitute = sqlSubstitute;
    this.cqlSubstitute = cqlSubstitute;
  }

  /**
   * Substitutes FIELD_NAME in sql query with expression that extracts only required comparison part
   *
   * @param sqlQuery original sql query
   * @return sql query with comparison part applied to FIELD_NAME or empty string if incoming sql query is empty
   */
  public String applyToSql(String sqlQuery) {
    return isBlank(sqlQuery) ? EMPTY : sqlQuery.replace("FIELD_NAME", sqlSubstitute);
  }

  /**
   * Substitutes FIELD_NAME in cql query with expression that extracts only required comparison part
   *
   * @param cqlQuery original cql query
   * @return cql query with comparison part applied to FIELD_NAME or empty string if incoming cql query is empty
   */
  public String applyToCql(String cqlQuery) {
    return isBlank(cqlQuery) ? EMPTY : cqlQuery.replace("FIELD_NAME", cqlSubstitute);
  }

}
