package org.folio.processing.matching.loader.query;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Implementation of LoadQuery that provides functionality to build an sql WHERE-clause,
 * allows to change table name and field name to adjust field reference,
 * applicable only for relational db schemas
 */
public class DefaultLoadQuery implements LoadQuery {

  private static final String FIELD_NAME = "FIELD_NAME";

  private String tableName;
  private String fieldName;
  private String whereClause;

  public DefaultLoadQuery(String tableName, String fieldName, String whereClause) {
    this.tableName = tableName;
    this.fieldName = fieldName;
    this.whereClause = whereClause;
  }

  @Override
  public String getSql() {
    return isBlank(whereClause) ? EMPTY : whereClause.replace(FIELD_NAME, getFieldReference());
  }

  @Override
  public String getCql() {
    throw new UnsupportedOperationException("CQl query is not applicable for non-jsonb fields");
  }

  public String getFieldReference() {
    return join(tableName, ".", fieldName);
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getWhereClause() {
    return whereClause;
  }

  public void setWhereClause(String whereClause) {
    this.whereClause = whereClause;
  }

}
