package org.folio.processing.matching.loader.query;

import org.folio.MatchDetail;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.Qualifier;

/**
 * Helper class that allows to build sql and cql queries based on MatchCriterion and apply Qualifier
 */
public class QueryHolder {

  private String sqlQuery;
  private String cqlQuery;

  public QueryHolder(Value value, MatchDetail.MatchCriterion matchCriterion) {
    MatchingCondition matchingCondition = MatchingCondition.valueOf(matchCriterion.name());
    this.sqlQuery = matchingCondition.constructSqlWhereClause(value);
    this.cqlQuery = matchingCondition.constructCqlQuery(value);
  }

  public QueryHolder applyQualifier(Qualifier qualifier) {
    return this
      .applyQualifierByType(qualifier)
      .applyQualifierComparisonPart(qualifier);
  }

  public QueryHolder applyQualifierByType(Qualifier qualifier) {
    if (qualifier != null && qualifier.getQualifierType() != null && qualifier.getQualifierValue() != null) {
      QualifierFilter filter = QualifierFilter.valueOf(qualifier.getQualifierType().name());
      sqlQuery += filter.getSqlFilter(qualifier.getQualifierValue());
      cqlQuery += filter.getCqlFilter(qualifier.getQualifierValue());
    }
    return this;
  }

  public QueryHolder applyQualifierComparisonPart(Qualifier qualifier) {
    if (qualifier != null && qualifier.getComparisonPart() != null) {
      QualifierComparisonPart comparisonPart = QualifierComparisonPart.valueOf(qualifier.getComparisonPart().name());
      sqlQuery = comparisonPart.applyToSql(sqlQuery);
      cqlQuery = comparisonPart.applyToCql(cqlQuery);
    }
    return this;
  }

  public String getSqlQuery() {
    return sqlQuery;
  }

  public String getCqlQuery() {
    return cqlQuery;
  }
}
