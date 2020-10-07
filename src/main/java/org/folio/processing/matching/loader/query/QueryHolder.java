package org.folio.processing.matching.loader.query;

import org.folio.MatchDetail;
import org.folio.processing.value.DateValue;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.Qualifier;

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.countMatches;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.split;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;
import static org.folio.processing.value.Value.ValueType.LIST;
import static org.folio.processing.value.Value.ValueType.STRING;

/**
 * Helper class that allows to build sql and cql queries based on MatchCriterion and apply Qualifier
 */
public class QueryHolder {

  private static final String TABLE_NAME = "TABLE_NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final String ARRAY_SIGN = "[]";

  private String sqlQuery;
  private String cqlQuery;
  private Value value;

  public QueryHolder(Value value, MatchDetail.MatchCriterion matchCriterion) {
    this.value = value;
    if (value.getType() == Value.ValueType.DATE) {
      this.sqlQuery = constructDateRangeSQLQuery((DateValue) value);
      this.cqlQuery = constructDateRangeCQLQuery((DateValue) value);
    } else {
      MatchingCondition matchingCondition = MatchingCondition.valueOf(matchCriterion.name());
      this.sqlQuery = matchingCondition.constructSqlWhereClause(value);
      this.cqlQuery = matchingCondition.constructCqlQuery(value);
    }
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

  public QueryHolder replaceFieldReference(String fieldPath, boolean isJson) {
    return this
      .replaceSqlFieldReference(fieldPath, isJson)
      .replaceCqlFieldReference(fieldPath, isJson);
  }

  public QueryHolder replaceSqlFieldReference(String fieldPath, boolean isJson) {
    sqlQuery = isJson ? replaceJsonFieldNameForSQLQuery(fieldPath) : replaceNonJsonFieldNameForSqlQuery(fieldPath);
    return this;
  }

  public QueryHolder replaceCqlFieldReference(String fieldPath, boolean isJson) {
    cqlQuery = isJson ? replaceJsonFieldNameForCQLQuery(fieldPath) : EMPTY;
    return this;
  }

  public String getSqlQuery() {
    return sqlQuery;
  }

  public String getCqlQuery() {
    return cqlQuery;
  }

  private String replaceJsonFieldNameForSQLQuery(String fieldPath) {
    String fieldReference;
    String arrayJoin = EMPTY;
    // TODO provide support for searching in nested arrays
    if (shouldJoinArray(fieldPath)) {
      arrayJoin = prependArrayJoin(TABLE_NAME, fieldPath);
      fieldReference = "field" + getFieldReference(substringAfter(fieldPath, ARRAY_SIGN));
    } else {
      fieldReference = join(TABLE_NAME, ".jsonb", getFieldReference(fieldPath));
    }
    return arrayJoin + sqlQuery.replace(FIELD_NAME, fieldReference);
  }

  private String replaceNonJsonFieldNameForSqlQuery(String fieldName) {
    return sqlQuery.replace(FIELD_NAME, join(TABLE_NAME, ".", fieldName));
  }

  private boolean shouldJoinArray(String jsonPath) {
    int count = countMatches(jsonPath, ARRAY_SIGN);
    if (count > 1) {
      throw new UnsupportedOperationException("Searching in nested arrays is not supported");
    } else {
      return count == 1;
    }
  }

  private String prependArrayJoin(String tableName, String jsonbFieldPath) {
    StringBuilder arrayJoin = new StringBuilder("CROSS JOIN LATERAL ");
    if (jsonbFieldPath.endsWith(ARRAY_SIGN)) {
      arrayJoin.append("jsonb_array_elements_text(");
    } else {
      arrayJoin.append("jsonb_array_elements(");
    }
    arrayJoin
      .append(tableName)
      .append(".jsonb")
      .append(getFieldReference(substringBefore(jsonbFieldPath, ARRAY_SIGN) + ARRAY_SIGN))
      .append(") fields(field) ");
    return arrayJoin.toString();
  }

  private String getFieldReference(String jsonbFieldPath) {
    StringBuilder fieldReference = new StringBuilder(EMPTY);
    List<String> tokens = asList(split(jsonbFieldPath, "."));
    for (int i = 0; i < tokens.size(); i++) {
      if (i == tokens.size() - 1 && !tokens.get(i).endsWith(ARRAY_SIGN)) {
        fieldReference
          .append(" ->> '")
          .append(tokens.get(i))
          .append("'");
      } else {
        fieldReference
          .append(" -> '")
          .append(tokens.get(i).replace(ARRAY_SIGN, EMPTY))
          .append("'");
      }
    }
    return fieldReference.toString();
  }

  private String constructDateRangeSQLQuery(DateValue value) {
    return format("WHERE FIELD_NAME >= '%s' AND FIELD_NAME <= '%sT23:59:59.999'", value.getFromDate(), value.getToDate());
  }

  private String constructDateRangeCQLQuery(DateValue value) {
    return format("FIELD_NAME >= \"%s\" AND FIELD_NAME <= \"%sT23:59:59.999\"", value.getFromDate(), value.getToDate());
  }

  private String replaceJsonFieldNameForCQLQuery(String fieldPath) {
    if (fieldPath.contains(ARRAY_SIGN)) {
      if (fieldPath.endsWith(ARRAY_SIGN)) {
        return constructCQLFilterByArrayStringValue(fieldPath);
      } else {
        return constructCQLFilterByFieldValueOfArrayElement(fieldPath);
      }
    }
    return cqlQuery.replace(FIELD_NAME, fieldPath);
  }

  private String constructCQLFilterByArrayStringValue(String fieldPath) {
    return fieldPath.replace(ARRAY_SIGN, EMPTY) + "=\\\"" + escapeSpecialCharacters(value.getValue().toString()) + "\\\"";
  }

  private String constructCQLFilterByFieldValueOfArrayElement(String fieldPath) {
    if (value.getType() == STRING) {
      return constructCQLFilterByFieldValueOfArrayElement(fieldPath, value.getValue().toString());
    } else if (value.getType() == LIST) {
      ListValue listValue = (ListValue) value;
      List<String> conditions = listValue.getValue().stream()
        .map(val -> constructCQLFilterByFieldValueOfArrayElement(fieldPath, val))
        .collect(Collectors.toList());
      return join(conditions, " AND ");
    }
    return EMPTY;
  }

  private String constructCQLFilterByFieldValueOfArrayElement(String fieldPath, String value) {
    return substringBefore(fieldPath, ARRAY_SIGN) + "=\"\\\""
      + substringAfter(fieldPath, ARRAY_SIGN + ".") +
      "\\\":\\\"" + escapeSpecialCharacters(value) + "\\\"\"";
  }

  private String escapeSpecialCharacters(String value) {
    return value.replaceAll("([*?\"^])", "\\\\$0");
  }
}
