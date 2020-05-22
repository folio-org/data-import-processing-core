package org.folio.processing.matching.loader.query;

import org.apache.commons.lang3.StringUtils;
import org.folio.MatchDetail;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;

import java.util.List;

import static org.folio.processing.value.Value.ValueType.DATE;
import static org.folio.processing.value.Value.ValueType.LIST;
import static org.folio.processing.value.Value.ValueType.STRING;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;

/**
 * Provides functionality to build LoadQuery based on match details and matching value
 */
public class LoadQueryBuilder {

  private LoadQueryBuilder() {
  }

  private static final String JSON_PATH_SEPARATOR = ".";

  /**
   * Builds LoadQuery,
   * applicable only for STRING, LIST and DATE value types,
   * applicable only for VALUE_FROM_RECORD data type,
   * currently supports building query only by single field (support for loading MARC records will be added later)
   *
   * @param value value to match against
   * @param matchDetail match detail
   * @return LoadQuery or null if query cannot be built
   */
  public static LoadQuery build(Value value, MatchDetail matchDetail) {
    if (value != null && (value.getType() == STRING || value.getType() == LIST || value.getType() == DATE)) {
      MatchExpression matchExpression = matchDetail.getExistingMatchExpression();
      if (matchExpression != null && matchExpression.getDataValueType() == VALUE_FROM_RECORD) {
        List<Field> fields = matchExpression.getFields();
        if (fields != null && fields.size() == 1) {
          String fieldPath = fields.get(0).getValue();
          String tableName = StringUtils.substringBefore(fieldPath, JSON_PATH_SEPARATOR);
          String fieldName = StringUtils.substringAfter(fieldPath, JSON_PATH_SEPARATOR);
          QueryHolder queryHolder = new QueryHolder(value, matchDetail.getMatchCriterion())
            .applyQualifier(matchExpression.getQualifier())
            .replaceFieldReference(fieldName, true);
          return new DefaultJsonLoadQuery(tableName, queryHolder.getSqlQuery(), queryHolder.getCqlQuery());
        }
        // TODO Support loading of MARC records
      }
    }
    return null;
  }

}
