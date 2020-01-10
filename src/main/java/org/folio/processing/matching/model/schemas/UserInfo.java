package org.folio.processing.matching.model.schemas;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * User information
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "firstName",
  "lastName",
  "userName"
})
public class UserInfo {

  /**
   * User first name
   */
  @JsonProperty("firstName")
  @JsonPropertyDescription("User first name")
  private String firstName;
  /**
   * User last name
   */
  @JsonProperty("lastName")
  @JsonPropertyDescription("User last name")
  private String lastName;
  /**
   * User name (nickname)
   */
  @JsonProperty("userName")
  @JsonPropertyDescription("User name (nickname)")
  private String userName;

  /**
   * User first name
   */
  @JsonProperty("firstName")
  public String getFirstName() {
    return firstName;
  }

  /**
   * User first name
   */
  @JsonProperty("firstName")
  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public UserInfo withFirstName(String firstName) {
    this.firstName = firstName;
    return this;
  }

  /**
   * User last name
   */
  @JsonProperty("lastName")
  public String getLastName() {
    return lastName;
  }

  /**
   * User last name
   */
  @JsonProperty("lastName")
  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public UserInfo withLastName(String lastName) {
    this.lastName = lastName;
    return this;
  }

  /**
   * User name (nickname)
   */
  @JsonProperty("userName")
  public String getUserName() {
    return userName;
  }

  /**
   * User name (nickname)
   */
  @JsonProperty("userName")
  public void setUserName(String userName) {
    this.userName = userName;
  }

  public UserInfo withUserName(String userName) {
    this.userName = userName;
    return this;
  }

}
