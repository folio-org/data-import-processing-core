package org.folio.processing.matching.model.schemas;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Match Profile static value details
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "staticValueType",
  "text",
  "number",
  "dateType",
  "fromDate",
  "toDate",
  "exactDate"
})
public class StaticValueDetails {

  /**
   * Static value type enum
   */
  @JsonProperty("staticValueType")
  @JsonPropertyDescription("Static value type enum")
  private StaticValueDetails.StaticValueType staticValueType;
  /**
   * Only if incomingStaticValueType = TEXT
   */
  @JsonProperty("text")
  @JsonPropertyDescription("Only if incomingStaticValueType = TEXT")
  private String text;
  /**
   * Only if incomingStaticValueType = NUMBER
   */
  @JsonProperty("number")
  @JsonPropertyDescription("Only if incomingStaticValueType = NUMBER")
  private Integer number;
  /**
   * Date types enum
   */
  @JsonProperty("dateType")
  @JsonPropertyDescription("Date types enum")
  private StaticValueDetails.DateType dateType;
  /**
   * Only if incomingStaticValueType = DATE and incomingDateType = RANGE
   */
  @JsonProperty("fromDate")
  @JsonPropertyDescription("Only if incomingStaticValueType = DATE and incomingDateType = RANGE")
  private Date fromDate;
  /**
   * Only if incomingStaticValueType = DATE and incomingDateType = RANGE
   */
  @JsonProperty("toDate")
  @JsonPropertyDescription("Only if incomingStaticValueType = DATE and incomingDateType = RANGE")
  private Date toDate;
  /**
   * Only if incomingStaticValueType = DATE and incomingDateType = EXACT_DATE
   */
  @JsonProperty("exactDate")
  @JsonPropertyDescription("Only if incomingStaticValueType = DATE and incomingDateType = EXACT_DATE")
  private Date exactDate;

  /**
   * Static value type enum
   */
  @JsonProperty("staticValueType")
  public StaticValueDetails.StaticValueType getStaticValueType() {
    return staticValueType;
  }

  /**
   * Static value type enum
   */
  @JsonProperty("staticValueType")
  public void setStaticValueType(StaticValueDetails.StaticValueType staticValueType) {
    this.staticValueType = staticValueType;
  }

  public StaticValueDetails withStaticValueType(StaticValueDetails.StaticValueType staticValueType) {
    this.staticValueType = staticValueType;
    return this;
  }

  /**
   * Only if incomingStaticValueType = TEXT
   */
  @JsonProperty("text")
  public String getText() {
    return text;
  }

  /**
   * Only if incomingStaticValueType = TEXT
   */
  @JsonProperty("text")
  public void setText(String text) {
    this.text = text;
  }

  public StaticValueDetails withText(String text) {
    this.text = text;
    return this;
  }

  /**
   * Only if incomingStaticValueType = NUMBER
   */
  @JsonProperty("number")
  public Integer getNumber() {
    return number;
  }

  /**
   * Only if incomingStaticValueType = NUMBER
   */
  @JsonProperty("number")
  public void setNumber(Integer number) {
    this.number = number;
  }

  public StaticValueDetails withNumber(Integer number) {
    this.number = number;
    return this;
  }

  /**
   * Date types enum
   */
  @JsonProperty("dateType")
  public StaticValueDetails.DateType getDateType() {
    return dateType;
  }

  /**
   * Date types enum
   */
  @JsonProperty("dateType")
  public void setDateType(StaticValueDetails.DateType dateType) {
    this.dateType = dateType;
  }

  public StaticValueDetails withDateType(StaticValueDetails.DateType dateType) {
    this.dateType = dateType;
    return this;
  }

  /**
   * Only if incomingStaticValueType = DATE and incomingDateType = RANGE
   */
  @JsonProperty("fromDate")
  public Date getFromDate() {
    return fromDate;
  }

  /**
   * Only if incomingStaticValueType = DATE and incomingDateType = RANGE
   */
  @JsonProperty("fromDate")
  public void setFromDate(Date fromDate) {
    this.fromDate = fromDate;
  }

  public StaticValueDetails withFromDate(Date fromDate) {
    this.fromDate = fromDate;
    return this;
  }

  /**
   * Only if incomingStaticValueType = DATE and incomingDateType = RANGE
   */
  @JsonProperty("toDate")
  public Date getToDate() {
    return toDate;
  }

  /**
   * Only if incomingStaticValueType = DATE and incomingDateType = RANGE
   */
  @JsonProperty("toDate")
  public void setToDate(Date toDate) {
    this.toDate = toDate;
  }

  public StaticValueDetails withToDate(Date toDate) {
    this.toDate = toDate;
    return this;
  }

  /**
   * Only if incomingStaticValueType = DATE and incomingDateType = EXACT_DATE
   */
  @JsonProperty("exactDate")
  public Date getExactDate() {
    return exactDate;
  }

  /**
   * Only if incomingStaticValueType = DATE and incomingDateType = EXACT_DATE
   */
  @JsonProperty("exactDate")
  public void setExactDate(Date exactDate) {
    this.exactDate = exactDate;
  }

  public StaticValueDetails withExactDate(Date exactDate) {
    this.exactDate = exactDate;
    return this;
  }

  public enum DateType {

    RANGE("RANGE"),
    EXACT_DATE("EXACT_DATE");
    private final String value;
    private static final Map<String, DateType> CONSTANTS = new HashMap<>();

    static {
      for (StaticValueDetails.DateType c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }

    private DateType(String value) {
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
    public static StaticValueDetails.DateType fromValue(String value) {
      StaticValueDetails.DateType constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }

  }

  public enum StaticValueType {

    TEXT("TEXT"),
    NUMBER("NUMBER"),
    DATE("DATE");
    private final String value;
    private static final Map<String, StaticValueType> CONSTANTS = new HashMap<>();

    static {
      for (StaticValueDetails.StaticValueType c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }

    private StaticValueType(String value) {
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
    public static StaticValueDetails.StaticValueType fromValue(String value) {
      StaticValueDetails.StaticValueType constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }

  }

}
