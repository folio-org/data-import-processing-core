package org.folio.processing.matching.model.schemas;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.List;

/**
 * tags
 * <p>
 * List of simple tags that can be added to an object
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "tagList"
})
public class Tags {

  /**
   * List of tags
   */
  @JsonProperty("tagList")
  @JsonPropertyDescription("List of tags")
  @Valid
  private List<String> tagList = new ArrayList<>();

  /**
   * List of tags
   */
  @JsonProperty("tagList")
  public List<String> getTagList() {
    return tagList;
  }

  /**
   * List of tags
   */
  @JsonProperty("tagList")
  public void setTagList(List<String> tagList) {
    this.tagList = tagList;
  }

  public Tags withTagList(List<String> tagList) {
    this.tagList = tagList;
    return this;
  }

}
