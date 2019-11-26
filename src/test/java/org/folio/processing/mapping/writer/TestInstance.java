package org.folio.processing.mapping.writer;

public class TestInstance {

  private String id;
  private String indexTitle;

  public TestInstance() {
  }

  public TestInstance(String id) {
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
