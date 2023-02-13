package org.folio.processing.mapping.manager;

public class TestOrder {

  private String id;
  private String vendor;

  TestOrder() {
  }

  public TestOrder(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public String getVendor() {
    return vendor;
  }

  public void setVendor(String vendor) {
    this.vendor = vendor;
  }
}
