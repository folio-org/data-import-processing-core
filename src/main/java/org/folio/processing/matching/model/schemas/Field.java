package org.folio.processing.matching.model.schemas;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import javax.validation.constraints.NotNull;

/**
 * Record field definition for marc and non marc records
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "label",
  "value"
})
public class Field {

  /**
   * Label from UI form which describes field or subfield in record
   * (Required)
   */
  @JsonProperty("label")
  @JsonPropertyDescription("Label from UI form which describes field or subfield in record")
  @NotNull
  private String label;
  /**
   * Field or subfield name in record
   * (Required)
   */
  @JsonProperty("value")
  @JsonPropertyDescription("Field or subfield name in record")
  @NotNull
  private String value;

  /**
   * Label from UI form which describes field or subfield in record
   * (Required)
   */
  @JsonProperty("label")
  public String getLabel() {
    return label;
  }

  /**
   * Label from UI form which describes field or subfield in record
   * (Required)
   */
  @JsonProperty("label")
  public void setLabel(String label) {
    this.label = label;
  }

  public Field withLabel(String label) {
    this.label = label;
    return this;
  }

  /**
   * Field or subfield name in record
   * (Required)
   */
  @JsonProperty("value")
  public String getValue() {
    return value;
  }

  /**
   * Field or subfield name in record
   * (Required)
   */
  @JsonProperty("value")
  public void setValue(String value) {
    this.value = value;
  }

  public Field withValue(String value) {
    this.value = value;
    return this;
  }

}
