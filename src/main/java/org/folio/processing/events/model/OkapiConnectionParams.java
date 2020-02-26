package org.folio.processing.events.model;

import java.util.Map;

public final class OkapiConnectionParams {
  private String okapiUrl;
  private String tenantId;
  private String token;
  private Map<String, String> headers;

  public OkapiConnectionParams() {
  }

  public OkapiConnectionParams(Map<String, String> okapiHeaders) {
    this.okapiUrl = okapiHeaders.getOrDefault("x-okapi-url", "localhost");
    this.tenantId = okapiHeaders.getOrDefault("x-okapi-tenant", "");
    this.token = okapiHeaders.getOrDefault("x-okapi-token", "dummy");
    this.headers = okapiHeaders;
  }

  public OkapiConnectionParams withTenantId(String tenantId) {
    this.tenantId = tenantId;
    return this;
  }

  public OkapiConnectionParams withToken(String token) {
    this.token = token;
    return this;
  }

  public OkapiConnectionParams withOkapiUrl(String okapiUrl) {
    this.okapiUrl = okapiUrl;
    return this;
  }

  public String getOkapiUrl() {
    return okapiUrl;
  }

  public void setOkapiUrl(String okapiUrl) {
    this.okapiUrl = okapiUrl;
  }

  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }
}
