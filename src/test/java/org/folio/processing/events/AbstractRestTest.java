package org.folio.processing.events;

import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.Rule;

public abstract class AbstractRestTest {
  protected final String TENANT_ID = "diku";
  protected final String TOKEN = "token";
  private int PORT = NetworkUtils.nextFreePort();
  protected final String OKAPI_URL = "http://localhost:" + PORT;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .port(PORT)
      .notifier(new Slf4jNotifier(true)));
}
