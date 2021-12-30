package org.folio.processing.events;

import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Rule;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;

public abstract class AbstractRestTest {
  protected final String TENANT_ID = "diku";
  protected final String TOKEN = "token";
  private int PORT = nextFreePort();
  protected final String OKAPI_URL = "http://localhost:" + PORT;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .port(PORT)
      .notifier(new Slf4jNotifier(true)));

  public static int nextFreePort() {
    int maxTries = 10000;
    int port = ThreadLocalRandom.current().nextInt(49152 , 65535);
    while (true) {
      if (isLocalPortFree(port)) {
        return port;
      } else {
        port = ThreadLocalRandom.current().nextInt(49152 , 65535);
      }
      maxTries--;
      if(maxTries == 0){
        return 8081;
      }
    }
  }

  public static boolean isLocalPortFree(int port) {
    try {
      new ServerSocket(port).close();
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
