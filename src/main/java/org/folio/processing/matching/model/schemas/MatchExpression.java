package org.folio.processing.matching.model.schemas;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Match expression
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "dataValueType",
  "fields",
  "staticValueDetails",
  "qualifier"
})
public class MatchExpression {

  /**
   * Enum of data value types
   * (Required)
   */
  @JsonProperty("dataValueType")
  @JsonPropertyDescription("Enum of data value types")
  @NotNull
  private MatchExpression.DataValueType dataValueType;
  /**
   * Only if dataValueType = VALUE_FROM_RECORD
   */
  @JsonProperty("fields")
  @JsonPropertyDescription("Only if dataValueType = VALUE_FROM_RECORD")
  @Valid
  private List<Field> fields = new ArrayList<>();
  /**
   * Match Profile static value details
   */
  @JsonProperty("staticValueDetails")
  @JsonPropertyDescription("Match Profile static value details")
  @Valid
  private StaticValueDetails staticValueDetails;
  /**
   * Match expression qualifier
   */
  @JsonProperty("qualifier")
  @JsonPropertyDescription("Match expression qualifier")
  @Valid
  private Qualifier qualifier;

  /**
   * Enum of data value types
   * (Required)
   */
  @JsonProperty("dataValueType")
  public MatchExpression.DataValueType getDataValueType() {
    return dataValueType;
  }

  /**
   * Enum of data value types
   * (Required)
   */
  @JsonProperty("dataValueType")
  public void setDataValueType(MatchExpression.DataValueType dataValueType) {
    this.dataValueType = dataValueType;
  }

  public MatchExpression withDataValueType(MatchExpression.DataValueType dataValueType) {
    this.dataValueType = dataValueType;
    return this;
  }

  /**
   * Only if dataValueType = VALUE_FROM_RECORD
   */
  @JsonProperty("fields")
  public List<Field> getFields() {
    return fields;
  }

  /**
   * Only if dataValueType = VALUE_FROM_RECORD
   */
  @JsonProperty("fields")
  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

  public MatchExpression withFields(List<Field> fields) {
    this.fields = fields;
    return this;
  }

  /**
   * Match Profile static value details
   */
  @JsonProperty("staticValueDetails")
  public StaticValueDetails getStaticValueDetails() {
    return staticValueDetails;
  }

  /**
   * Match Profile static value details
   */
  @JsonProperty("staticValueDetails")
  public void setStaticValueDetails(StaticValueDetails staticValueDetails) {
    this.staticValueDetails = staticValueDetails;
  }

  public MatchExpression withStaticValueDetails(StaticValueDetails staticValueDetails) {
    this.staticValueDetails = staticValueDetails;
    return this;
  }

  /**
   * Match expression qualifier
   */
  @JsonProperty("qualifier")
  public Qualifier getQualifier() {
    return qualifier;
  }

  /**
   * Match expression qualifier
   */
  @JsonProperty("qualifier")
  public void setQualifier(Qualifier qualifier) {
    this.qualifier = qualifier;
  }

  public MatchExpression withQualifier(Qualifier qualifier) {
    this.qualifier = qualifier;
    return this;
  }

  public enum DataValueType {

    VALUE_FROM_RECORD("VALUE_FROM_RECORD"),
    STATIC_VALUE("STATIC_VALUE");
    private final String value;
    private static final Map<String, DataValueType> CONSTANTS = new HashMap<>();

    static {
      for (MatchExpression.DataValueType c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }

    private DataValueType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    @JsonValue
    public String value() {
      return this.value;
    }

    @JsonCreator
    public static MatchExpression.DataValueType fromValue(String value) {
      MatchExpression.DataValueType constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }

  }

}
