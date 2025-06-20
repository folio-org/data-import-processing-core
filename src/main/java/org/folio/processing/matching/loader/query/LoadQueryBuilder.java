package org.folio.processing.matching.loader.query;

import io.vertx.core.json.Json;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.MatchDetail;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;

import java.util.ArrayList;
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

  private static final Logger LOGGER = LogManager.getLogger(LoadQueryBuilder.class);
  private static final String JSON_PATH_SEPARATOR = ".";
  private static final String IDENTIFIER_TYPE_ID = "identifierTypeId";
  private static final String IDENTIFIER_TYPE_VALUE = "instance.identifiers[].value";
  private static final String IDENTIFIER_INDIVIDUAL_CQL_QUERY = "identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"%s\\\"\"";

  /**
   * Builds LoadQuery,
   * applicable only for STRING, LIST and DATE value types,
   * applicable only for VALUE_FROM_RECORD data type,
   * currently supports building query only by single field (support for loading MARC records will be added later)
   *
   * @param value       value to match against
   * @param matchDetail match detail
   * @return LoadQuery or null if query cannot be built
   */
  public static LoadQuery build(Value<?> value, MatchDetail matchDetail) {
    if (value != null && (value.getType() == STRING || value.getType() == LIST || value.getType() == DATE)) {
      MatchExpression matchExpression = matchDetail.getExistingMatchExpression();
      if (matchExpression != null && matchExpression.getDataValueType() == VALUE_FROM_RECORD) {
        List<Field> fields = matchExpression.getFields();
        if (fields != null && !fields.isEmpty()) {
          String fieldPath = fields.getFirst().getValue();
          String tableName = StringUtils.substringBefore(fieldPath, JSON_PATH_SEPARATOR);
          String fieldName = StringUtils.substringAfter(fieldPath, JSON_PATH_SEPARATOR);
          QueryHolder mainQuery = new QueryHolder(value, matchDetail.getMatchCriterion())
            .applyQualifier(matchExpression.getQualifier())
            .replaceFieldReference(fieldName, true);
          if (fields.size() > 1) {
            Field additionalField = fields.get(1);
            String additionalFieldName = StringUtils.substringBefore(fieldName, JSON_PATH_SEPARATOR)
              + JSON_PATH_SEPARATOR
              + additionalField.getLabel();
            QueryHolder additionalQuery = new QueryHolder(StringValue.of(additionalField.getValue()), matchDetail.getMatchCriterion())
              .replaceFieldReference(additionalFieldName, true);
            mainQuery.applyAdditionalCondition(additionalQuery);
            // TODO provide all the requirements for MODDATAIMP-592 and refactor code block below
            if(checkIfIdentifierTypeExists(matchDetail, fieldPath, additionalField.getLabel())) {
              String cqlQuery = buildIdentifierCqlQuery(value, additionalField.getValue());
              mainQuery.setCqlQuery(cqlQuery);
              mainQuery.setSqlQuery(StringUtils.EMPTY);
            } else {
              LOGGER.debug("LoadQueryBuilder::build - Additional field does not match identifier type criteria: {} fieldPath: {}",
                additionalField.getLabel(), fieldPath);
            }
          }
          LOGGER.debug(() -> String.format("LoadQueryBuilder::build - Built LoadQuery for VALUE: ~| %s |~ MATCHDETAIL: ~| %s |~ CQL: ~| %s |~",
            Json.encode(value), Json.encode(matchDetail), mainQuery.getCqlQuery()));
          return new DefaultJsonLoadQuery(tableName, mainQuery.getSqlQuery(), mainQuery.getCqlQuery());
        }
      }
    }
    return null;
  }

  private static boolean checkIfIdentifierTypeExists(MatchDetail matchDetail, String fieldPath, String additionalFieldPath) {
    return matchDetail.getIncomingRecordType() == EntityType.MARC_BIBLIOGRAPHIC && matchDetail.getExistingRecordType() == EntityType.INSTANCE &&
      matchDetail.getMatchCriterion() == MatchDetail.MatchCriterion.EXACTLY_MATCHES && fieldPath.equals(IDENTIFIER_TYPE_VALUE) &&
      additionalFieldPath.equals(IDENTIFIER_TYPE_ID);
  }

  /**
   * Builds CQL query for identifier matching with individual AND conditions for each value
   *
   * @param value          the value to match against (can be STRING or LIST)
   * @param identifierTypeId the identifier type ID
   * @return CQL query string with individual AND conditions
   */
  private static String buildIdentifierCqlQuery(Value<?> value, String identifierTypeId) {
    if (value.getType() == STRING) {
      return String.format(IDENTIFIER_INDIVIDUAL_CQL_QUERY, identifierTypeId, escapeCqlValue(value.getValue().toString()));
    } else if (value.getType() == LIST) {
      List<String> conditions = new ArrayList<>();
      for (Object val : ((org.folio.processing.value.ListValue) value).getValue()) {
        conditions.add("(" + String.format(IDENTIFIER_INDIVIDUAL_CQL_QUERY, identifierTypeId, escapeCqlValue(val.toString())) + ")");
      }
      return String.join(" OR ", conditions);
    }
    return "";
  }

  /**
   * Escapes special characters in CQL values to prevent parsing errors
   *
   * @param value the value to escape
   * @return escaped value safe for CQL queries
   */
  private static String escapeCqlValue(String value) {
    // Escape backslashes first, then other special characters
    return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("(", "\\(")
                .replace(")", "\\)")
                .replace("*", "\\*")
                .replace("?", "\\?");
  }

}
