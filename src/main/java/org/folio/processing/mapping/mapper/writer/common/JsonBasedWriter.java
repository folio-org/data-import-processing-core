package org.folio.processing.mapping.mapper.writer.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.mapper.writer.AbstractWriter;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MapValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.EntityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.lang.String.format;

/**
 * A common Writer based on json. The idea is to hold Jackson's JsonNode and fill up it by incoming values in runtime.
 */
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
    TextNode textNode = new TextNode(stringValue.getValue());
    setValueByFieldPath(fieldPath, textNode);
  }

  @Override
  protected void writeListValue(String fieldPath, ListValue listValue) {
    ArrayNode arrayNode = objectMapper.valueToTree(listValue.getValue());
    setValueByFieldPath(fieldPath, arrayNode);
  }

  @Override
  protected void writeObjectValue(String fieldPath, MapValue mapValue) {
    JsonNode objectNode = objectMapper.valueToTree(mapValue.getValue());
    setValueByFieldPath(fieldPath, objectNode);
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
    FieldPathIterator fieldPathIterator = new FieldPathIterator(fieldPath);
    JsonNode parentNode = entityNode;
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
   * @param pathItem    item of the fieldPath
   * @param fieldValue  value of the field to set
   * @param parentNode  node where to set a fieldValue
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
}
