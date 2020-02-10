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

  /**
   * Builds LoadQuery,
   * applicable only for STRING and LIST value types,
   * applicable only for VALUE_FROM_RECORD data type,
   * currently supports building query only by single field (support for loading MARC records will be added later)
   *
   * @param value value to match against
   * @param matchDetail match detail
   * @return LoadQuery or null if query cannot be built
   */
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
      }
    }
    return null;
  }

}
