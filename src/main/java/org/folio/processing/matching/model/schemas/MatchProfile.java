package org.folio.processing.matching.model.schemas;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import javax.validation.constraints.Pattern;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Match Profile schema
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "id",
  "name",
  "description",
  "tags",
  "entityType",
  "incomingRecordType",
  "existingRecordType",
  "matchDetails",
  "deleted",
  "userInfo",
  "metadata"
})
public class MatchProfile {

  /**
   * Regexp pattern for UUID validation
   */
  @JsonProperty("id")
  @JsonPropertyDescription("Regexp pattern for UUID validation")
  @Pattern(regexp = "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[1-5][a-fA-F0-9]{3}-[89abAB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}$")
  private String id;
  /**
   * Match Profile name
   * (Required)
   */
  @JsonProperty("name")
  @JsonPropertyDescription("Match Profile name")
  @NotNull
  private String name;
  /**
   * Match Profile description
   */
  @JsonProperty("description")
  @JsonPropertyDescription("Match Profile description")
  private String description;
  /**
   * tags
   * <p>
   * List of simple tags that can be added to an object
   */
  @JsonProperty("tags")
  @JsonPropertyDescription("List of simple tags that can be added to an object")
  @Valid
  private Tags tags;
  /**
   * Entity type
   */
  @JsonProperty("entityType")
  @JsonPropertyDescription("Entity type")
  private String entityType;
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
   * Match profile details list
   */
  @JsonProperty("matchDetails")
  @JsonPropertyDescription("Match profile details list")
  @Valid
  private List<MatchDetail> matchDetails = new ArrayList<>();
  /**
   * Flag indicates that the Match Profile marked as deleted
   */
  @JsonProperty("deleted")
  @JsonPropertyDescription("Flag indicates that the Match Profile marked as deleted")
  private Boolean deleted = false;
  /**
   * User information
   */
  @JsonProperty("userInfo")
  @JsonPropertyDescription("User information")
  @Valid
  private UserInfo userInfo;
  /**
   * Metadata Schema
   * <p>
   * Metadata about creation and changes to records, provided by the server (client should not provide)
   */
  @JsonProperty("metadata")
  @JsonPropertyDescription("Metadata about creation and changes to records, provided by the server (client should not provide)")
  @Null
  @Valid
  private Metadata metadata;

  /**
   * Regexp pattern for UUID validation
   */
  @JsonProperty("id")
  public String getId() {
    return id;
  }

  /**
   * Regexp pattern for UUID validation
   */
  @JsonProperty("id")
  public void setId(String id) {
    this.id = id;
  }

  public MatchProfile withId(String id) {
    this.id = id;
    return this;
  }

  /**
   * Match Profile name
   * (Required)
   */
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  /**
   * Match Profile name
   * (Required)
   */
  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  public MatchProfile withName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Match Profile description
   */
  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  /**
   * Match Profile description
   */
  @JsonProperty("description")
  public void setDescription(String description) {
    this.description = description;
  }

  public MatchProfile withDescription(String description) {
    this.description = description;
    return this;
  }

  /**
   * tags
   * <p>
   * List of simple tags that can be added to an object
   */
  @JsonProperty("tags")
  public Tags getTags() {
    return tags;
  }

  /**
   * tags
   * <p>
   * List of simple tags that can be added to an object
   */
  @JsonProperty("tags")
  public void setTags(Tags tags) {
    this.tags = tags;
  }

  public MatchProfile withTags(Tags tags) {
    this.tags = tags;
    return this;
  }

  /**
   * Entity type
   */
  @JsonProperty("entityType")
  public String getEntityType() {
    return entityType;
  }

  /**
   * Entity type
   */
  @JsonProperty("entityType")
  public void setEntityType(String entityType) {
    this.entityType = entityType;
  }

  public MatchProfile withEntityType(String entityType) {
    this.entityType = entityType;
    return this;
  }

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

  public MatchProfile withIncomingRecordType(MatchProfile.IncomingRecordType incomingRecordType) {
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

  public MatchProfile withExistingRecordType(MatchProfile.ExistingRecordType existingRecordType) {
    this.existingRecordType = existingRecordType;
    return this;
  }

  /**
   * Match profile details list
   */
  @JsonProperty("matchDetails")
  public List<MatchDetail> getMatchDetails() {
    return matchDetails;
  }

  /**
   * Match profile details list
   */
  @JsonProperty("matchDetails")
  public void setMatchDetails(List<MatchDetail> matchDetails) {
    this.matchDetails = matchDetails;
  }

  public MatchProfile withMatchDetails(List<MatchDetail> matchDetails) {
    this.matchDetails = matchDetails;
    return this;
  }

  /**
   * Flag indicates that the Match Profile marked as deleted
   */
  @JsonProperty("deleted")
  public Boolean getDeleted() {
    return deleted;
  }

  /**
   * Flag indicates that the Match Profile marked as deleted
   */
  @JsonProperty("deleted")
  public void setDeleted(Boolean deleted) {
    this.deleted = deleted;
  }

  public MatchProfile withDeleted(Boolean deleted) {
    this.deleted = deleted;
    return this;
  }

  /**
   * User information
   */
  @JsonProperty("userInfo")
  public UserInfo getUserInfo() {
    return userInfo;
  }

  /**
   * User information
   */
  @JsonProperty("userInfo")
  public void setUserInfo(UserInfo userInfo) {
    this.userInfo = userInfo;
  }

  public MatchProfile withUserInfo(UserInfo userInfo) {
    this.userInfo = userInfo;
    return this;
  }

  /**
   * Metadata Schema
   * <p>
   * Metadata about creation and changes to records, provided by the server (client should not provide)
   */
  @JsonProperty("metadata")
  public Metadata getMetadata() {
    return metadata;
  }

  /**
   * Metadata Schema
   * <p>
   * Metadata about creation and changes to records, provided by the server (client should not provide)
   */
  @JsonProperty("metadata")
  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  public MatchProfile withMetadata(Metadata metadata) {
    this.metadata = metadata;
    return this;
  }

  public enum ExistingRecordType {

    INSTANCE("INSTANCE"),
    HOLDINGS("HOLDINGS"),
    ITEM("ITEM"),
    ORDER("ORDER"),
    INVOICE("INVOICE"),
    MARC_BIBLIOGRAPHIC("MARC_BIBLIOGRAPHIC"),
    MARC_HOLDINGS("MARC_HOLDINGS"),
    MARC_AUTHORITY("MARC_AUTHORITY");
    private final String value;
    private static final Map<String, ExistingRecordType> CONSTANTS = new HashMap<>();

    static {
      for (MatchProfile.ExistingRecordType c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }

    private ExistingRecordType(String value) {
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
    public static MatchProfile.ExistingRecordType fromValue(String value) {
      MatchProfile.ExistingRecordType constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }

  }

  public enum IncomingRecordType {

    DELIMITED("Delimited"),
    EDIFACT("EDIFACT"),
    MARC("MARC");
    private final String value;
    private static final Map<String, IncomingRecordType> CONSTANTS = new HashMap<>();

    static {
      for (MatchProfile.IncomingRecordType c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }

    private IncomingRecordType(String value) {
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
    public static MatchProfile.IncomingRecordType fromValue(String value) {
      MatchProfile.IncomingRecordType constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }

  }

}
