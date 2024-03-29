package org.folio.processing.matching.entities;

public class PartialError {
  private String id;
  private String error;

  public PartialError(String id, String error) {
    this.id = id;
    this.error = error;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }
}
