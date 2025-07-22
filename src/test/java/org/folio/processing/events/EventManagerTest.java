package org.folio.processing.events;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.kafka.KafkaContainer;

@RunWith(VertxUnitRunner.class)
public class EventManagerTest {
  private static final String KAFKA_ENV = "folio";

  @ClassRule
  public static KafkaContainer kafkaContainer = new KafkaContainer(TestUtil.KAFKA_CONTAINER_NAME);
  private static KafkaConfig kafkaConfig;

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  @BeforeClass
  public static void setUpClass() {
    kafkaConfig = KafkaConfig.builder()
        .kafkaHost(kafkaContainer.getHost())
        .kafkaPort(kafkaContainer.getFirstMappedPort() + "")
        .envId(KAFKA_ENV)
        .build();
  }

  @Test
  public void registerKafkaEventPublisher(TestContext context) {
    Vertx vertx = rule.vertx();
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, 100);
    context.assertEquals(1, EventManager.getEventPublishers().size());
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, 100);
    context.assertEquals(1, EventManager.getEventPublishers().size());
  }
}
