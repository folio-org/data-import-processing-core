package org.folio.processing.matching.model.schemas;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Match expression qualifier
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "qualifierType",
  "qualifierValue",
  "comparisonPart"
})
public class Qualifier {

  /**
   * Enum of qualifier types
   */
  @JsonProperty("qualifierType")
  @JsonPropertyDescription("Enum of qualifier types")
  private Qualifier.QualifierType qualifierType;
  /**
   * Qualifier value, only if qualifierType is specified
   */
  @JsonProperty("qualifierValue")
  @JsonPropertyDescription("Qualifier value, only if qualifierType is specified")
  private String qualifierValue;
  /**
   * Type enum of comparison part
   */
  @JsonProperty("comparisonPart")
  @JsonPropertyDescription("Type enum of comparison part")
  private Qualifier.ComparisonPart comparisonPart;

  /**
   * Enum of qualifier types
   */
  @JsonProperty("qualifierType")
  public Qualifier.QualifierType getQualifierType() {
    return qualifierType;
  }

  /**
   * Enum of qualifier types
   */
  @JsonProperty("qualifierType")
  public void setQualifierType(Qualifier.QualifierType qualifierType) {
    this.qualifierType = qualifierType;
  }

  public Qualifier withQualifierType(Qualifier.QualifierType qualifierType) {
    this.qualifierType = qualifierType;
    return this;
  }

  /**
   * Qualifier value, only if qualifierType is specified
   */
  @JsonProperty("qualifierValue")
  public String getQualifierValue() {
    return qualifierValue;
  }

  /**
   * Qualifier value, only if qualifierType is specified
   */
  @JsonProperty("qualifierValue")
  public void setQualifierValue(String qualifierValue) {
    this.qualifierValue = qualifierValue;
  }

  public Qualifier withQualifierValue(String qualifierValue) {
    this.qualifierValue = qualifierValue;
    return this;
  }

  /**
   * Type enum of comparison part
   */
  @JsonProperty("comparisonPart")
  public Qualifier.ComparisonPart getComparisonPart() {
    return comparisonPart;
  }

  /**
   * Type enum of comparison part
   */
  @JsonProperty("comparisonPart")
  public void setComparisonPart(Qualifier.ComparisonPart comparisonPart) {
    this.comparisonPart = comparisonPart;
  }

  public Qualifier withComparisonPart(Qualifier.ComparisonPart comparisonPart) {
    this.comparisonPart = comparisonPart;
    return this;
  }

  public enum ComparisonPart {

    NUMERICS_ONLY("NUMERICS_ONLY"),
    ALPHANUMERICS_ONLY("ALPHANUMERICS_ONLY");
    private final String value;
    private static final Map<String, ComparisonPart> CONSTANTS = new HashMap<>();

    static {
      for (Qualifier.ComparisonPart c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }

    private ComparisonPart(String value) {
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
    public static Qualifier.ComparisonPart fromValue(String value) {
      Qualifier.ComparisonPart constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }

  }

  public enum QualifierType {

    BEGINS_WITH("BEGINS_WITH"),
    ENDS_WITH("ENDS_WITH"),
    CONTAINS("CONTAINS");
    private final String value;
    private static final Map<String, QualifierType> CONSTANTS = new HashMap<>();

    static {
      for (Qualifier.QualifierType c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }

    private QualifierType(String value) {
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
    public static Qualifier.QualifierType fromValue(String value) {
      Qualifier.QualifierType constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }

  }

}
