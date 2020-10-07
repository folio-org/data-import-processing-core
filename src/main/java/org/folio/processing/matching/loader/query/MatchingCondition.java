package org.folio.processing.matching.loader.query;

import org.folio.processing.value.ListValue;
import org.folio.processing.value.Value;

import java.util.List;
import java.util.stream.Collectors;

import org.folio.MatchDetail.MatchCriterion;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.processing.value.Value.ValueType.LIST;
import static org.folio.processing.value.Value.ValueType.STRING;

/**
 * Allows to build sql and cql query structures according to the {@link MatchCriterion},
 * FIELD_NAME token is used to mark the place for field reference
 */
public enum MatchingCondition {

  // TODO provide expressions for missing cql queries
  EXACTLY_MATCHES("FIELD_NAME = '%s'",
    "FIELD_NAME == \"%s\""),
  EXISTING_VALUE_CONTAINS_INCOMING_VALUE("FIELD_NAME LIKE '%%%s%%'",
    "FIELD_NAME == \"*%s*\""),
  INCOMING_VALUE_CONTAINS_EXISTING_VALUE("'%s' LIKE CONCAT('%%', FIELD_NAME, '%%')",
    "FIELD_NAME any \"%s\""),
  EXISTING_VALUE_ENDS_WITH_INCOMING_VALUE("FIELD_NAME LIKE '%%%s'",
    "FIELD_NAME == \"*%s\""),
  INCOMING_VALUE_ENDS_WITH_EXISTING_VALUE("'%s' LIKE CONCAT('%%', FIELD_NAME)",
    EMPTY),
  EXISTING_VALUE_BEGINS_WITH_INCOMING_VALUE("FIELD_NAME LIKE '%s%%'",
    "FIELD_NAME == \"%s*\""),
  INCOMING_VALUE_BEGINS_WITH_EXISTING_VALUE("'%s' LIKE CONCAT(FIELD_NAME, '%%')",
    EMPTY);

  private String sqlCondition;
  private String cqlQuery;

  MatchingCondition(String sqlCondition, String cqlQuery) {
    this.sqlCondition = sqlCondition;
    this.cqlQuery = cqlQuery;
  }

  /**
   * Builds sql WHERE-clause passing an actual value to the sqlCondition structure
   *
   * @param value {@link Value} that should be applied in sql WHERE-clause,
   *              supports only STRING and LIST value types
   * @return sql WHERE-clause or an empty string if query cannot be build for passed Value type
   */
  public String constructSqlWhereClause(Value value) {
    String condition = constructConditionWithValue(value, sqlCondition);
    return isBlank(condition) ? EMPTY : "WHERE ".concat(condition);
  }

  /**
   * Builds cql query passing an actual value to the cqlQuery structure
   *
   * @param value {@link Value} that should be applied in cql query,
   *              currently supports only STRING and LIST value type
   * @return cql query or an empty string if no query structure is provided for {@link MatchCriterion}
   * or query cannot be built for specified value type
   */
  public String constructCqlQuery(Value value) {
    return constructConditionWithValue(value, cqlQuery);
  }

  private String constructConditionWithValue(Value value, String format) {
    String condition = EMPTY;
    if (value.getType() == STRING) {
      condition = format(format, value.getValue());
    } else if (value.getType() == LIST) {
      ListValue listValue = (ListValue) value;
      List<String> conditions = listValue.getValue().stream()
        .map(val -> format(format, val))
        .collect(Collectors.toList());
      condition = join("(", join(conditions, " OR "), ")");
    }
    return condition;
  }
}
