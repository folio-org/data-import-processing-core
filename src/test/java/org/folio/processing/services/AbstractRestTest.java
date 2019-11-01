package org.folio.processing.services;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.folio.processing.core.model.OkapiConnectionParams;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.Before;
import org.junit.Rule;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractRestTest {
  private final String TENANT_ID = "diku";
  private final String TOKEN = "token";
  private int PORT = NetworkUtils.nextFreePort();
  private final String HOST = "http://localhost:" + PORT;
  private final String PUBLISH_SERVICE_URL = HOST + "/pubsub/publish/";
  public OkapiConnectionParams okapiConnectionParams;
  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .port(PORT)
      .notifier(new Slf4jNotifier(true)));

  @Before
  public void setup() {
    Map<String, String> okapiHeaders = new HashMap<>();
    okapiHeaders.put("x-okapi-url", HOST);
    okapiHeaders.put("x-okapi-tenant", TENANT_ID);
    okapiHeaders.put("x-okapi-token", TOKEN);
    this.okapiConnectionParams = new OkapiConnectionParams(okapiHeaders);
    WireMock.stubFor(WireMock.post(PUBLISH_SERVICE_URL).willReturn(WireMock.ok()));
  }
}
