package org.folio.processing.matching.loader.query;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Implementation of LoadQuery that provides functionality to build an sql WHERE-clause and cql query,
 * allows to change table name to adjust field reference,
 * applicable only for querying data stored in 'jsonb' column
 */
public class DefaultJsonLoadQuery implements LoadQuery {

  private static final String TABLE_NAME = "TABLE_NAME";

  private String tableName;
  private String whereClause;
  private String cqlQuery;

  public DefaultJsonLoadQuery(String tableName, String whereClause, String cqlQuery) {
    this.tableName = tableName;
    this.whereClause = whereClause;
    this.cqlQuery = cqlQuery;
  }

  @Override
  public String getSql() {
    if (isBlank(whereClause)) {
      return EMPTY;
    }
    return whereClause.replace(TABLE_NAME, tableName);
  }

  @Override
  public String getCql() {
    return cqlQuery;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getWhereClause() {
    return whereClause;
  }

  public void setWhereClause(String whereClause) {
    this.whereClause = whereClause;
  }

  public void setCqlQuery(String cqlQuery) {
    this.cqlQuery = cqlQuery;
  }

}
