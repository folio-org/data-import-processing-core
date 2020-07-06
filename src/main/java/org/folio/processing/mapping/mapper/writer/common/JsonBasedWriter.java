package org.folio.processing.mapping.mapper.writer.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.mapper.writer.AbstractWriter;
import org.folio.processing.value.BooleanValue;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MapValue;
import org.folio.processing.value.RepeatableFieldValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.logging.log4j.util.Strings.EMPTY;

/**
 * A common Writer based on json. The idea is to hold Jackson's JsonNode and fill up it by incoming values in runtime.
 */
@SuppressWarnings("rawtypes")
public class JsonBasedWriter extends AbstractWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonBasedWriter.class);
  private ObjectMapper objectMapper = new ObjectMapper();
  private String entityType;
  private JsonNode entityNode;

  public JsonBasedWriter(EntityType entityType) {
    this.entityType = entityType.value();
  }

  @Override
  public void initialize(DataImportEventPayload eventPayload) throws IOException {
    if (eventPayload.getContext().containsKey(entityType)) {
      this.entityNode = new ObjectMapper().readTree(eventPayload.getContext().get(entityType));
    } else {
      throw new IllegalArgumentException("Can not initialize JsonBasedWriter. No suitable entity type found in context");
    }
  }

  @Override
  protected void writeStringValue(String fieldPath, StringValue stringValue) {
    if (isNotEmpty(stringValue.getValue())) {
      TextNode textNode = new TextNode(stringValue.getValue());
      setValueByFieldPath(fieldPath, textNode);
    }
  }

  @Override
  protected void writeListValue(String fieldPath, ListValue listValue) {
    if (listValue.getRepeatableFieldAction() == null) {
      ArrayNode arrayNode = objectMapper.valueToTree(listValue.getValue());
      setValueByFieldPath(fieldPath, arrayNode);
      return;
    }
    writeListValueByAction(fieldPath, listValue);
  }

  protected void writeListValueByAction(String fieldPath, ListValue listValue) {
    ArrayNode arrayValue = objectMapper.valueToTree(listValue.getValue());
    String pathForSearch = fieldPath.replace("[]", EMPTY);
    JsonNode foundNode = entityNode.findPath(pathForSearch);

    switch (listValue.getRepeatableFieldAction()) {
      case EXTEND_EXISTING:
        setValueByFieldPath(fieldPath, arrayValue);
        break;
      case EXCHANGE_EXISTING:
        removeByCurrentPath(pathForSearch, foundNode);
        setValueByFieldPath(fieldPath, arrayValue);
        break;
      case DELETE_INCOMING:
        ArrayNode arrayNode = (ArrayNode) foundNode;
        for (int i = 0; i < arrayNode.size(); i++) {
          if (listValue.getValue().contains(arrayNode.get(i).textValue())) {
            arrayNode.remove(i);
          }
        }
        break;
      case DELETE_EXISTING:
        removeByCurrentPath(pathForSearch, foundNode);
        break;
      default:
        throw new IllegalArgumentException("Can not define action type");
    }
  }

  @Override
  protected void writeObjectValue(String fieldPath, MapValue mapValue) {
    JsonNode objectNode = objectMapper.valueToTree(mapValue.getValue());
    setValueByFieldPath(fieldPath, objectNode);
  }

  private void writeValuesForRepeatableObject(JsonNode object, Map.Entry<String, Value> objectFields) {
    JsonNode field = MissingNode.getInstance();
    switch (objectFields.getValue().getType()) {
      case LIST:
      case MAP:
        field = objectMapper.valueToTree(objectFields.getValue().getValue());
        break;
      case STRING:
        field = new TextNode((String) objectFields.getValue().getValue());
        break;
      case BOOLEAN:
        BooleanValue bool = (BooleanValue) objectFields.getValue();
        MappingRule.BooleanFieldAction booleanFieldAction = bool.getValue();
        if (booleanFieldAction.equals(MappingRule.BooleanFieldAction.ALL_TRUE)) {
          field = BooleanNode.TRUE;
        } else if (booleanFieldAction.equals(MappingRule.BooleanFieldAction.ALL_FALSE)) {
          field = BooleanNode.FALSE;
        }
        break;
      case MISSING:
      case REPEATABLE:
      default:
        break;
    }
    if (!field.isMissingNode()) {
      setValueByFieldPath(objectFields.getKey().substring(objectFields.getKey().lastIndexOf('.') + 1), field, object);
    }
  }

  private void setRepeatableValueByAction(MappingRule.RepeatableFieldAction action, String repeatableFieldPath, JsonNode currentObject) {
    String currentPath = repeatableFieldPath.replace("[]", EMPTY);
    JsonNode pathObject = entityNode.findPath(currentPath);
    switch (action) {
      case EXTEND_EXISTING:
        setValueByFieldPath(repeatableFieldPath, currentObject);
        break;
      case EXCHANGE_EXISTING:
        removeByCurrentPath(repeatableFieldPath, pathObject);
        setValueByFieldPath(repeatableFieldPath, currentObject);
        break;
      case DELETE_INCOMING:
        if (pathObject.isArray()) {
          ArrayNode arrayNode = (ArrayNode) pathObject;
          for (int i = 0; i < arrayNode.size(); i++) {
            if (arrayNode.get(i).equals(currentObject)) {
              arrayNode.remove(i);
            }
          }
        } else if (pathObject.equals(currentObject) && !pathObject.isMissingNode()) {
          ((ObjectNode) entityNode).remove(currentPath);
        }
        break;
      case DELETE_EXISTING:
        removeByCurrentPath(currentPath, pathObject);
        break;
      default:
        throw new IllegalArgumentException("Can not define action type");
    }
  }

  @Override
  protected void writeRepeatableValue(String repeatableFieldPath, RepeatableFieldValue value) {
    List<Map<String, Value>> repeatableFields = value.getValue();
    MappingRule.RepeatableFieldAction action = value.getRepeatableFieldAction();
    processIfRepeatableFieldsAreEmpty(repeatableFieldPath, value, repeatableFields);
    for (Map<String, Value> subfield : repeatableFields) {
      JsonNode currentObject = objectMapper.createObjectNode();
      for (Map.Entry<String, Value> objectFields : subfield.entrySet()) {
        writeValuesForRepeatableObject(currentObject, objectFields);
      }
      setRepeatableValueByAction(action, repeatableFieldPath, currentObject);
    }
  }

  @Override
  protected void writeBooleanValue(String fieldPath, BooleanValue value) {
    BooleanNode booleanNode;
    MappingRule.BooleanFieldAction action = value.getValue();
    if (action.equals(MappingRule.BooleanFieldAction.ALL_TRUE)) {
      booleanNode = BooleanNode.TRUE;
    } else if (action.equals(MappingRule.BooleanFieldAction.ALL_FALSE)) {
      booleanNode = BooleanNode.FALSE;
    } else {
      return;
    }
    setValueByFieldPath(fieldPath, booleanNode);
  }

  /**
   * The method does traversing by field path from top to bottom.
   * Each iteration the method creates a ContainerNode for the next path if it does not exist in parent node (see #addContainerNode)
   * If a path item is the last, then method sets value to the parent node (see #setValue)
   *
   * @param fieldPath  field path
   * @param fieldValue value of the field, JsonNode is the parent node of ValueNode and ContainerNode
   */
  private void setValueByFieldPath(String fieldPath, JsonNode fieldValue) {
    setValueByFieldPath(fieldPath, fieldValue, entityNode);
  }

  private void setValueByFieldPath(String fieldPath, JsonNode fieldValue, JsonNode node) {
    FieldPathIterator fieldPathIterator = new FieldPathIterator(fieldPath);
    JsonNode parentNode = node;
    while (fieldPathIterator.hasNext()) {
      FieldPathIterator.PathItem pathItem = fieldPathIterator.next();
      if (fieldPathIterator.hasNext()) {
        parentNode = addContainerNode(pathItem, parentNode);
      } else {
        setValueNode(pathItem, fieldValue, parentNode);
      }
    }
  }

  /**
   * The idea of method is to check existence of ContainerNode for the given path item.
   * If ContainerNode does not exist, then method adds it for the given path item.
   *
   * @param pathItem   path item
   * @param parentNode parent node where to check existence of ContainerNode for path item
   * @return JsonNode with ContainerNode in
   */
  private JsonNode addContainerNode(FieldPathIterator.PathItem pathItem, JsonNode parentNode) {
    JsonNode childNode = parentNode.findPath(pathItem.getName());
    if (childNode.isMissingNode() || childNode.isNull()) {
      childNode = pathItem.isObject() ? parentNode.with(pathItem.getName()) : parentNode.withArray(pathItem.getName());
    }
    return childNode;
  }

  /**
   * The method sets a given fieldValue into parentNode based on type of pathItem.
   *
   * @param pathItem   item of the fieldPath
   * @param fieldValue value of the field to set
   * @param parentNode node where to set a fieldValue
   */
  private void setValueNode(FieldPathIterator.PathItem pathItem, JsonNode fieldValue, JsonNode parentNode) {
    if (parentNode.isArray()) {
      throw new IllegalStateException("Wrong field path: Array can not be a parent node");
    }

    if (pathItem.isArray() && fieldValue.isArray()) {
      ArrayNode arrayNode = (ArrayNode) parentNode.withArray(pathItem.getName());
      for (JsonNode jsonNode : fieldValue) {
        arrayNode.add(jsonNode);
      }
    } else if (pathItem.isArray() && fieldValue.isObject()) {
      ArrayNode arrayNode = (ArrayNode) parentNode.withArray(pathItem.getName());
      arrayNode.add(fieldValue);
    } else if (pathItem.isObject() && !fieldValue.isArray()) {
      ((ObjectNode) parentNode).set(pathItem.getName(), fieldValue);
    } else {
      throw new IllegalStateException("Types mismatch: Type of path item and type of the value are incompatible");
    }
  }

  @Override
  public DataImportEventPayload getResult(DataImportEventPayload eventPayload) {
    try {
      String jsonEntity = objectMapper.writeValueAsString(this.entityNode);
      eventPayload.getContext().put(entityType, jsonEntity);
    } catch (JsonProcessingException e) {
      LOGGER.error(format("Can not write entity node to json string. Cause:  %s", e));
      throw new IllegalStateException(e);
    }
    return eventPayload;
  }

  private void removeByCurrentPath(String currentPath, JsonNode pathObject) {
    if (!pathObject.isMissingNode()) {
      ((ObjectNode) entityNode).remove(currentPath);
    }
  }

  private void processIfRepeatableFieldsAreEmpty(String repeatableFieldPath, RepeatableFieldValue value, List<Map<String, Value>> repeatableFields) {
    if (repeatableFields.isEmpty() && value.getRepeatableFieldAction() == MappingRule.RepeatableFieldAction.DELETE_EXISTING) {
      String currentPath = repeatableFieldPath.replace("[]", EMPTY);
      removeByCurrentPath(currentPath, entityNode.findPath(currentPath));
    }
  }
}
