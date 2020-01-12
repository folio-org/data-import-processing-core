package org.folio.processing.mapping.mapper.writer.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.mapper.writer.AbstractWriter;
import org.folio.processing.mapping.model.MappingProfile;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

import static java.lang.String.format;

public class JsonBasedWriter extends AbstractWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonBasedWriter.class);
  private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private String entityType;
  private JsonNode entityNode;

  public JsonBasedWriter(MappingProfile.EntityType entityType) {
    this.entityType = entityType.value();
  }

  @Override
  public void initialize(EventContext eventContext) throws IOException {
    if (eventContext.getObjects().containsKey(entityType)) {
      this.entityNode = new ObjectMapper().readTree(eventContext.getObjects().get(entityType));
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
    ArrayNode arrayNode = OBJECT_MAPPER.valueToTree(listValue.getValue());
    setValueByFieldPath(fieldPath, arrayNode);
  }

  private void setValueByFieldPath(String fieldPath, JsonNode fieldValue) {
    FieldPathIterator fieldPathIterator = new FieldPathIterator(fieldPath);
    JsonNode parentNode = entityNode;
    while (fieldPathIterator.hasNext()) {
      FieldPathIterator.PathItem pathItem = fieldPathIterator.next();
      if (fieldPathIterator.hasNext()) {
        parentNode = addContainerNode(parentNode, pathItem);
      } else {
        addValueNode(fieldValue, parentNode, pathItem);
      }
    }
  }

  private void addValueNode(JsonNode fieldValue, JsonNode parentNode, FieldPathIterator.PathItem pathItem) {
    if (parentNode.isObject()) {
      JsonNode childNode = parentNode.findPath(pathItem.getName());
      if (childNode.isMissingNode() || childNode.isNull()) {
        if (pathItem.isArray()) {
          if (fieldValue.isArray()) {
            ArrayNode arrayNode = (ArrayNode) parentNode.withArray(pathItem.getName());
            Iterator<JsonNode> iterator = fieldValue.iterator();
            while (iterator.hasNext()) {
              arrayNode.add(iterator.next());
            }
          } else {
            throw new IllegalStateException("Only array can be written to array field");
          }
        } else if (pathItem.isObject()) {
            if (fieldValue.isTextual()) {
              ((ObjectNode) parentNode).set(pathItem.getName(), fieldValue);
            } else {
              throw new IllegalStateException("Only text can be written to object field");
            }
        }
      } else {
        if (pathItem.isArray()) {
          if (fieldValue.isArray()) {
            ArrayNode arrayNode = (ArrayNode) parentNode.withArray(pathItem.getName());
            Iterator<JsonNode> iterator = fieldValue.iterator();
            while (iterator.hasNext()) {
              arrayNode.add(iterator.next());
            }
          } else {
             throw new IllegalStateException("Only array can be written to array field");
          }
        } else if (pathItem.isObject()) {
          throw new IllegalStateException(format("Wrong field path: [%s]. Cause: Override is forbidden", pathItem.getName()));
        }
      }
    } else if (parentNode.isArray()) {
      throw new IllegalStateException(format("Wrong field path: [%s]. Cause: Array can not be a parent node", pathItem.getName()));
    }
  }

  private JsonNode addContainerNode(JsonNode parentNode, FieldPathIterator.PathItem pathItem) {
    JsonNode childNode = parentNode.findPath(pathItem.getName());
    if (childNode.isMissingNode() || childNode.isNull()) {
      if (pathItem.isArray()) {
        return parentNode.withArray(pathItem.getName());
      } else if (pathItem.isObject()) {
        return parentNode.with(pathItem.getName());
      }
    }
    return childNode;
  }

  @Override
  public EventContext getResult(EventContext eventContext) {
    try {
      String jsonEntity = OBJECT_MAPPER.writeValueAsString(this.entityNode);
      eventContext.getObjects().put(entityType, jsonEntity);
    } catch (JsonProcessingException e) {
      LOGGER.error(format("Can not write entity node to json string. Cause:  %s", e.getCause()));
      throw new RuntimeException(e);
    }
    return eventContext;
  }
}
