package org.folio.processing.matching.loader.query;

import org.folio.processing.matching.model.schemas.Qualifier;
import org.folio.processing.matching.model.schemas.Qualifier.QualifierType;

import static java.lang.String.format;

/**
 * Allows to build additional sql or cql filter according to the {@link QualifierType},
 * FIELD_NAME token is used to mark the place for field reference
 */
public enum QualifierFilter {

  CONTAINS(" AND LIKE '%%%s%%'", " AND FIELD_NAME = '*%s*'"),
  ENDS_WITH(" AND LIKE '%%%s'", " AND FIELD_NAME = '*%s'"),
  BEGINS_WITH(" AND LIKE '%s%%'", " AND FIELD_NAME = '%s*'");

  private String sqlFilter;
  private String cqlFilter;

  QualifierFilter(String sqlFilter, String cqlFilter) {
    this.sqlFilter = sqlFilter;
    this.cqlFilter = cqlFilter;
  }

  /**
   * Builds sql filter passing an actual {@link Qualifier} value to the sql filter structure
   *
   * @param qualifierValue value that should be applied in sql filter
   * @return additional sql filter
   */
  public String getSqlFilter(String qualifierValue) {
    return format(sqlFilter, qualifierValue);
  }

  /**
   * Builds cql filter passing an actual {@link Qualifier} value to the cql filter structure
   *
   * @param qualifierValue value that should be applied in sql filter
   * @return additional cql filter
   */
  public String getCqlFilter(String qualifierValue) {
    return format(cqlFilter, qualifierValue);
  }

}
