package org.folio.processing.matching.model.schemas;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

/**
 * Match profile detail
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "incomingRecordType",
  "existingRecordType",
  "incomingMatchExpression",
  "matchCriterion",
  "existingMatchExpression",
  "booleanOperation"
})
public class MatchDetail {

  /**
   * Data Types Enum
   * (Required)
   */
  @JsonProperty("incomingRecordType")
  @JsonPropertyDescription("Data Types Enum")
  @NotNull
  private MatchProfile.IncomingRecordType incomingRecordType;
  /**
   * Enum of existing record types to match
   * (Required)
   */
  @JsonProperty("existingRecordType")
  @JsonPropertyDescription("Enum of existing record types to match")
  @NotNull
  private MatchProfile.ExistingRecordType existingRecordType;
  /**
   * Match expression
   * (Required)
   */
  @JsonProperty("incomingMatchExpression")
  @JsonPropertyDescription("Match expression")
  @Valid
  @NotNull
  private MatchExpression incomingMatchExpression;
  /**
   * Matching criterion types enum
   * (Required)
   */
  @JsonProperty("matchCriterion")
  @JsonPropertyDescription("Matching criterion types enum")
  @NotNull
  private MatchDetail.MatchCriterion matchCriterion;
  /**
   * Match expression
   * (Required)
   */
  @JsonProperty("existingMatchExpression")
  @JsonPropertyDescription("Match expression")
  @Valid
  @NotNull
  private MatchExpression existingMatchExpression;
  /**
   * Used to add sub-matches to an existing match profile
   */
  @JsonProperty("booleanOperation")
  @JsonPropertyDescription("Used to add sub-matches to an existing match profile")
  private MatchDetail.BooleanOperation booleanOperation;

  /**
   * Data Types Enum
   * (Required)
   */
  @JsonProperty("incomingRecordType")
  public MatchProfile.IncomingRecordType getIncomingRecordType() {
    return incomingRecordType;
  }

  /**
   * Data Types Enum
   * (Required)
   */
  @JsonProperty("incomingRecordType")
  public void setIncomingRecordType(MatchProfile.IncomingRecordType incomingRecordType) {
    this.incomingRecordType = incomingRecordType;
  }

  public MatchDetail withIncomingRecordType(MatchProfile.IncomingRecordType incomingRecordType) {
    this.incomingRecordType = incomingRecordType;
    return this;
  }

  /**
   * Enum of existing record types to match
   * (Required)
   */
  @JsonProperty("existingRecordType")
  public MatchProfile.ExistingRecordType getExistingRecordType() {
    return existingRecordType;
  }

  /**
   * Enum of existing record types to match
   * (Required)
   */
  @JsonProperty("existingRecordType")
  public void setExistingRecordType(MatchProfile.ExistingRecordType existingRecordType) {
    this.existingRecordType = existingRecordType;
  }

  public MatchDetail withExistingRecordType(MatchProfile.ExistingRecordType existingRecordType) {
    this.existingRecordType = existingRecordType;
    return this;
  }

  /**
   * Match expression
   * (Required)
   */
  @JsonProperty("incomingMatchExpression")
  public MatchExpression getIncomingMatchExpression() {
    return incomingMatchExpression;
  }

  /**
   * Match expression
   * (Required)
   */
  @JsonProperty("incomingMatchExpression")
  public void setIncomingMatchExpression(MatchExpression incomingMatchExpression) {
    this.incomingMatchExpression = incomingMatchExpression;
  }

  public MatchDetail withIncomingMatchExpression(MatchExpression incomingMatchExpression) {
    this.incomingMatchExpression = incomingMatchExpression;
    return this;
  }

  /**
   * Matching criterion types enum
   * (Required)
   */
  @JsonProperty("matchCriterion")
  public MatchDetail.MatchCriterion getMatchCriterion() {
    return matchCriterion;
  }

  /**
   * Matching criterion types enum
   * (Required)
   */
  @JsonProperty("matchCriterion")
  public void setMatchCriterion(MatchDetail.MatchCriterion matchCriterion) {
    this.matchCriterion = matchCriterion;
  }

  public MatchDetail withMatchCriterion(MatchDetail.MatchCriterion matchCriterion) {
    this.matchCriterion = matchCriterion;
    return this;
  }

  /**
   * Match expression
   * (Required)
   */
  @JsonProperty("existingMatchExpression")
  public MatchExpression getExistingMatchExpression() {
    return existingMatchExpression;
  }

  /**
   * Match expression
   * (Required)
   */
  @JsonProperty("existingMatchExpression")
  public void setExistingMatchExpression(MatchExpression existingMatchExpression) {
    this.existingMatchExpression = existingMatchExpression;
  }

  public MatchDetail withExistingMatchExpression(MatchExpression existingMatchExpression) {
    this.existingMatchExpression = existingMatchExpression;
    return this;
  }

  /**
   * Used to add sub-matches to an existing match profile
   */
  @JsonProperty("booleanOperation")
  public MatchDetail.BooleanOperation getBooleanOperation() {
    return booleanOperation;
  }

  /**
   * Used to add sub-matches to an existing match profile
   */
  @JsonProperty("booleanOperation")
  public void setBooleanOperation(MatchDetail.BooleanOperation booleanOperation) {
    this.booleanOperation = booleanOperation;
  }

  public MatchDetail withBooleanOperation(MatchDetail.BooleanOperation booleanOperation) {
    this.booleanOperation = booleanOperation;
    return this;
  }

  public enum BooleanOperation {

    AND("AND"),
    AND_NOT("AND NOT"),
    OR("OR");
    private final String value;
    private final static Map<String, BooleanOperation> CONSTANTS = new HashMap<String, BooleanOperation>();

    static {
      for (MatchDetail.BooleanOperation c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }

    private BooleanOperation(String value) {
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
    public static MatchDetail.BooleanOperation fromValue(String value) {
      MatchDetail.BooleanOperation constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }

  }

  public enum MatchCriterion {

    EXACTLY_MATCHES("EXACTLY_MATCHES"),
    EXISTING_VALUE_CONTAINS_INCOMING_VALUE("EXISTING_VALUE_CONTAINS_INCOMING_VALUE"),
    INCOMING_VALUE_CONTAINS_EXISTING_VALUE("INCOMING_VALUE_CONTAINS_EXISTING_VALUE"),
    EXISTING_VALUE_BEGINS_WITH_INCOMING_VALUE("EXISTING_VALUE_BEGINS_WITH_INCOMING_VALUE"),
    INCOMING_VALUE_BEGINS_WITH_EXISTING_VALUE("INCOMING_VALUE_BEGINS_WITH_EXISTING_VALUE"),
    EXISTING_VALUE_ENDS_WITH_INCOMING_VALUE("EXISTING_VALUE_ENDS_WITH_INCOMING_VALUE"),
    INCOMING_VALUE_ENDS_WITH_EXISTING_VALUE("INCOMING_VALUE_ENDS_WITH_EXISTING_VALUE");
    private final String value;
    private final static Map<String, MatchCriterion> CONSTANTS = new HashMap<String, MatchCriterion>();

    static {
      for (MatchDetail.MatchCriterion c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }

    private MatchCriterion(String value) {
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
    public static MatchDetail.MatchCriterion fromValue(String value) {
      MatchDetail.MatchCriterion constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }

  }

}
