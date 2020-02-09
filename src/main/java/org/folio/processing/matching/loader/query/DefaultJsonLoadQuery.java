package org.folio.processing.matching.loader.query;

import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.countMatches;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.split;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;

/**
 * Implementation of LoadQuery that provides functionality to build an sql WHERE-clause and cql query,
 * allows to change table name and jsonFieldPath to adjust field reference,
 * applicable only for querying data stored in 'jsonb' column
 */
public class DefaultJsonLoadQuery implements LoadQuery {

  private static final String FIELD_NAME = "FIELD_NAME";
  private static final String ARRAY_SIGN = "[]";

  private String tableName;
  private String jsonFieldPath;
  private String whereClause;
  private String cqlQuery;

  public DefaultJsonLoadQuery(String tableName, String jsonFieldPath, String whereClause, String cqlQuery) {
    this.tableName = tableName;
    this.jsonFieldPath = jsonFieldPath;
    this.whereClause = whereClause;
    this.cqlQuery = cqlQuery;
  }

  @Override
  public String getSql() {
    if (isBlank(whereClause)) {
      return EMPTY;
    }
    String fieldReference;
    String arrayJoin = EMPTY;
    // TODO provide support for searching in nested arrays
    if (shouldJoinArray(jsonFieldPath)) {
      arrayJoin = prependArrayJoin(tableName, jsonFieldPath);
      fieldReference = "field" + getFieldReference(substringAfter(jsonFieldPath, ARRAY_SIGN));
    } else {
      fieldReference = join(tableName, ".jsonb", getFieldReference(jsonFieldPath));
    }
    return arrayJoin + whereClause.replace(FIELD_NAME, fieldReference);
  }

  @Override
  public String getCql() {
    return cqlQuery.replace(FIELD_NAME, jsonFieldPath);
  }

  private boolean shouldJoinArray(String jsonPath) {
    int count = countMatches(jsonPath, ARRAY_SIGN);
    if (count > 1) {
      throw new UnsupportedOperationException("Searching in nested arrays is not supported");
    } else if (count == 1) {
      return true;
    }
    return false;
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

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getJsonFieldPath() {
    return jsonFieldPath;
  }

  public void setJsonFieldPath(String jsonFieldPath) {
    this.jsonFieldPath = jsonFieldPath;
  }

  public String getWhereClause() {
    return whereClause;
  }

  public void setWhereClause(String whereClause) {
    this.whereClause = whereClause;
  }

  public String getCqlQuery() {
    return cqlQuery;
  }

  public void setCqlQuery(String cqlQuery) {
    this.cqlQuery = cqlQuery;
  }

}
