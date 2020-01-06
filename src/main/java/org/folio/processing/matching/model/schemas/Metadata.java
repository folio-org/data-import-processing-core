package org.folio.processing.matching.model.schemas;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.Date;

/**
 * Metadata Schema
 * <p>
 * Metadata about creation and changes to records, provided by the server (client should not provide)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "createdDate",
  "createdByUserId",
  "createdByUsername",
  "updatedDate",
  "updatedByUserId",
  "updatedByUsername"
})
public class Metadata {

  /**
   * Date and time when the record was created
   * (Required)
   */
  @JsonProperty("createdDate")
  @JsonPropertyDescription("Date and time when the record was created")
  @NotNull
  private Date createdDate;
  /**
   * ID of the user who created the record
   * (Required)
   */
  @JsonProperty("createdByUserId")
  @JsonPropertyDescription("ID of the user who created the record")
  @Pattern(regexp = "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$")
  @NotNull
  private String createdByUserId;
  /**
   * Username of the user who created the record (when available)
   */
  @JsonProperty("createdByUsername")
  @JsonPropertyDescription("Username of the user who created the record (when available)")
  private String createdByUsername;
  /**
   * Date and time when the record was last updated
   */
  @JsonProperty("updatedDate")
  @JsonPropertyDescription("Date and time when the record was last updated")
  private Date updatedDate;
  /**
   * ID of the user who last updated the record
   */
  @JsonProperty("updatedByUserId")
  @JsonPropertyDescription("ID of the user who last updated the record")
  @Pattern(regexp = "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$")
  private String updatedByUserId;
  /**
   * Username of the user who last updated the record (when available)
   */
  @JsonProperty("updatedByUsername")
  @JsonPropertyDescription("Username of the user who last updated the record (when available)")
  private String updatedByUsername;

  /**
   * Date and time when the record was created
   * (Required)
   */
  @JsonProperty("createdDate")
  public Date getCreatedDate() {
    return createdDate;
  }

  /**
   * Date and time when the record was created
   * (Required)
   */
  @JsonProperty("createdDate")
  public void setCreatedDate(Date createdDate) {
    this.createdDate = createdDate;
  }

  public Metadata withCreatedDate(Date createdDate) {
    this.createdDate = createdDate;
    return this;
  }

  /**
   * ID of the user who created the record
   * (Required)
   */
  @JsonProperty("createdByUserId")
  public String getCreatedByUserId() {
    return createdByUserId;
  }

  /**
   * ID of the user who created the record
   * (Required)
   */
  @JsonProperty("createdByUserId")
  public void setCreatedByUserId(String createdByUserId) {
    this.createdByUserId = createdByUserId;
  }

  public Metadata withCreatedByUserId(String createdByUserId) {
    this.createdByUserId = createdByUserId;
    return this;
  }

  /**
   * Username of the user who created the record (when available)
   */
  @JsonProperty("createdByUsername")
  public String getCreatedByUsername() {
    return createdByUsername;
  }

  /**
   * Username of the user who created the record (when available)
   */
  @JsonProperty("createdByUsername")
  public void setCreatedByUsername(String createdByUsername) {
    this.createdByUsername = createdByUsername;
  }

  public Metadata withCreatedByUsername(String createdByUsername) {
    this.createdByUsername = createdByUsername;
    return this;
  }

  /**
   * Date and time when the record was last updated
   */
  @JsonProperty("updatedDate")
  public Date getUpdatedDate() {
    return updatedDate;
  }

  /**
   * Date and time when the record was last updated
   */
  @JsonProperty("updatedDate")
  public void setUpdatedDate(Date updatedDate) {
    this.updatedDate = updatedDate;
  }

  public Metadata withUpdatedDate(Date updatedDate) {
    this.updatedDate = updatedDate;
    return this;
  }

  /**
   * ID of the user who last updated the record
   */
  @JsonProperty("updatedByUserId")
  public String getUpdatedByUserId() {
    return updatedByUserId;
  }

  /**
   * ID of the user who last updated the record
   */
  @JsonProperty("updatedByUserId")
  public void setUpdatedByUserId(String updatedByUserId) {
    this.updatedByUserId = updatedByUserId;
  }

  public Metadata withUpdatedByUserId(String updatedByUserId) {
    this.updatedByUserId = updatedByUserId;
    return this;
  }

  /**
   * Username of the user who last updated the record (when available)
   */
  @JsonProperty("updatedByUsername")
  public String getUpdatedByUsername() {
    return updatedByUsername;
  }

  /**
   * Username of the user who last updated the record (when available)
   */
  @JsonProperty("updatedByUsername")
  public void setUpdatedByUsername(String updatedByUsername) {
    this.updatedByUsername = updatedByUsername;
  }

  public Metadata withUpdatedByUsername(String updatedByUsername) {
    this.updatedByUsername = updatedByUsername;
    return this;
  }

}
