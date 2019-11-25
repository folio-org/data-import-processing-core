package org.folio.processing.mapping.writer;

public class Instance {

  private String id;
  private String indexTitle;

  public Instance() {
  }

  public Instance(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public String getIndexTitle() {
    return indexTitle;
  }

  public void setIndexTitle(String indexTitle) {
    this.indexTitle = indexTitle;
  }
}
