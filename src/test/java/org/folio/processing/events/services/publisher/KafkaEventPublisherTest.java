package org.folio.processing.events.services.publisher;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.ObserveKeyValues;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.jaxrs.model.Event;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class KafkaEventPublisherTest {
  private static final String KAFKA_ENV = "test-env";
  private static final String OKAPI_URL = "http://localhost";
  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "stub-token";

  @ClassRule
  public static EmbeddedKafkaCluster kafkaCluster = provisionWith(EmbeddedKafkaClusterConfig.useDefaults());

  private static KafkaConfig kafkaConfig;
  private Vertx vertx = Vertx.vertx();
  private KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100);;

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
  public void shouldPublishPayload() throws InterruptedException {
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_COMPLETED.value())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withContext(new HashMap<>() {{
        put("recordId", UUID.randomUUID().toString());
      }});

    CompletableFuture<Event> future = eventPublisher.publish(eventPayload);

    String topicToObserve = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, DI_COMPLETED.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());

    Event obtainedEvent = Json.decodeValue(observedValues.get(0), Event.class);
    DataImportEventPayload actualPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertEquals(eventPayload, actualPayload);

    assertFalse(future.isCompletedExceptionally());
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenPayloadIsNull() throws ExecutionException, InterruptedException {
    CompletableFuture<Event> future = eventPublisher.publish(null);
    assertTrue(future.isCompletedExceptionally());
    future.get();
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenPayloadParameterIsNull() throws ExecutionException, InterruptedException {
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_COMPLETED.value())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(null)
      .withContext(new HashMap<>() {{
        put("recordId", UUID.randomUUID().toString());
      }});

    CompletableFuture<Event> future = eventPublisher.publish(eventPayload);
    assertTrue(future.isCompletedExceptionally());
    future.get();
  }

}
