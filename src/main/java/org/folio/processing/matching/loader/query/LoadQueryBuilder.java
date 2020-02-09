package org.folio.processing.matching.loader.query;

import org.apache.commons.lang3.StringUtils;
import org.folio.processing.matching.model.schemas.Field;
import org.folio.processing.matching.model.schemas.MatchDetail;
import org.folio.processing.matching.model.schemas.MatchExpression;
import org.folio.processing.value.Value;
import java.util.List;

import static org.folio.processing.matching.model.schemas.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.processing.value.Value.ValueType.LIST;
import static org.folio.processing.value.Value.ValueType.STRING;

/**
 * Provides functionality to build LoadQuery based on match details and matching value
 */
public class LoadQueryBuilder {

  private LoadQueryBuilder() {
  }

  private static final String JSON_PATH_SEPARATOR = ".";

  public static LoadQuery build(Value value, MatchDetail matchDetail) {
    if (value != null && (value.getType() == STRING || value.getType() == LIST)) {
      MatchExpression matchExpression = matchDetail.getExistingMatchExpression();
      if (matchExpression != null && matchExpression.getDataValueType() == VALUE_FROM_RECORD) {
        List<Field> fields = matchExpression.getFields();
        if (fields != null && fields.size() == 1) {
          QueryHolder queryHolder = new QueryHolder(value, matchDetail.getMatchCriterion())
            .applyQualifier(matchExpression.getQualifier());
          String fieldPath = fields.get(0).getValue();
          String tableName = StringUtils.substringBefore(fieldPath, JSON_PATH_SEPARATOR);
          String fieldName = StringUtils.substringAfter(fieldPath, JSON_PATH_SEPARATOR);
          return new DefaultJsonLoadQuery(tableName, fieldName, queryHolder.getSqlQuery(), queryHolder.getCqlQuery());
        }
        // TODO Support loading of MARC records
        // Building query by multiple fields is not supported
      }
      // Load query is not applicable for STATIC_VALUE data type
    }
    // Load query can be built only for STRING or LIST value types
    return null;
  }

}
