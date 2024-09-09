package org.folio.processing.events;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import org.folio.kafka.KafkaConfig;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;

@RunWith(VertxUnitRunner.class)
public class EventManagerTest {
  private static final String KAFKA_ENV = "folio";

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  @ClassRule
  public static EmbeddedKafkaCluster kafkaCluster = provisionWith(EmbeddedKafkaClusterConfig.useDefaults());
  private static KafkaConfig kafkaConfig;

  @BeforeClass
  public static void setUpClass() {
    String[] hostAndPort = kafkaCluster.getBrokerList().split(":");
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
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