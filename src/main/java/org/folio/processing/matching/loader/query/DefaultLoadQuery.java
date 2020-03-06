package org.folio.processing.matching.loader.query;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Implementation of LoadQuery that provides functionality to build an sql WHERE-clause,
 * allows to change table name to adjust field reference,
 * applicable only for relational db schemas
 */
public class DefaultLoadQuery implements LoadQuery {

  private static final String TABLE_NAME = "TABLE_NAME";

  private String tableName;
  private String whereClause;

  public DefaultLoadQuery(String tableName, String whereClause) {
    this.tableName = tableName;
    this.whereClause = whereClause;
  }

  @Override
  public String getSql() {
    return isBlank(whereClause) ? EMPTY : whereClause.replace(TABLE_NAME, tableName);
  }

  @Override
  public String getCql() {
    throw new UnsupportedOperationException("CQl query is not applicable for non-jsonb fields");
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

}
