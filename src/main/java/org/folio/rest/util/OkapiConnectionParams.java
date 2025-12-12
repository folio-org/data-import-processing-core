package org.folio.rest.util;
import io.vertx.core.Vertx;

import java.util.Map;

public final class OkapiConnectionParams {

  public static final String OKAPI_URL_HEADER = "x-okapi-url";
  public static final String OKAPI_TENANT_HEADER = "x-okapi-tenant";
  public static final String OKAPI_TOKEN_HEADER = "x-okapi-token";
  public static final String USER_ID_HEADER = "userId";
  public static final String OKAPI_REQUEST_ID_HEADER = "x-okapi-request-id";
  private String okapiUrl;
  private String tenantId;
  private String token;
  private Map<String, String> headers;
  private Vertx vertx;
  private int timeout = 2000;

  public OkapiConnectionParams() {
  }

  public OkapiConnectionParams(Vertx vertx) {
    this.vertx = vertx;
  }

  public OkapiConnectionParams(Map<String, String> okapiHeaders, Vertx vertx) {
    this.okapiUrl = okapiHeaders.getOrDefault(OKAPI_URL_HEADER, "localhost");
    this.tenantId = okapiHeaders.getOrDefault(OKAPI_TENANT_HEADER, "");
    this.token = okapiHeaders.getOrDefault(OKAPI_TOKEN_HEADER, "dummy");
    this.headers = okapiHeaders;
    this.vertx = vertx;
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

  public Vertx getVertx() {
    return vertx;
  }

  public void setVertx(Vertx vertx) {
    this.vertx = vertx;
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }
}
