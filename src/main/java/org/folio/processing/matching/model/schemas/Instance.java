package org.folio.processing.matching.model.schemas;

public class Instance {

  private String id;
  private String title;

  public Instance() {
  }

  public Instance(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }
}
