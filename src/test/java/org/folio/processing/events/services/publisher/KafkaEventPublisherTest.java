package org.folio.processing.events.services.publisher;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.TestUtil;
import org.folio.rest.jaxrs.model.Event;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.kafka.KafkaContainer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.processing.events.services.publisher.KafkaEventPublisher.CHUNK_ID_HEADER;
import static org.folio.processing.events.services.publisher.KafkaEventPublisher.PERMISSIONS_HEADER;
import static org.folio.processing.events.services.publisher.KafkaEventPublisher.RECORD_ID_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.USER_ID_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class KafkaEventPublisherTest {
  private static final String KAFKA_ENV = "folio";
  private static final String OKAPI_URL = "http://localhost";
  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "stub-token";

  @ClassRule
  public static KafkaContainer kafkaContainer = new KafkaContainer(TestUtil.KAFKA_CONTAINER_NAME);
  private static KafkaConfig kafkaConfig;
  private static Properties consumerConfig = new Properties();
  private Vertx vertx = Vertx.vertx();

  @BeforeClass
  public static void setUpClass() {
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(kafkaContainer.getHost())
      .kafkaPort(kafkaContainer.getFirstMappedPort() + "")
      .envId(KAFKA_ENV)
      .build();
    kafkaConfig.getConsumerProps().forEach((key, value) -> {
      if (value != null) {
        consumerConfig.put(key, value);
      }
    });
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
  }

  @Test
  public void shouldPublishPayload() throws Exception {
    var tenant = "shouldPublishPayload";
    String expectedPermissionsHeader = JsonArray.of("test-permission").encode();
    String expectedUserId = UUID.randomUUID().toString();
    String expectedRecordId = UUID.randomUUID().toString();
    String expectedChunkId = UUID.randomUUID().toString();
    try(KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      DataImportEventPayload eventPayload = new DataImportEventPayload()
        .withEventType(DI_COMPLETED.value())
        .withOkapiUrl(OKAPI_URL)
        .withTenant(tenant)
        .withToken(TOKEN)
        .withContext(new HashMap<>() {{
          put(RECORD_ID_HEADER, expectedRecordId);
          put(CHUNK_ID_HEADER, expectedChunkId);
          put(PERMISSIONS_HEADER, expectedPermissionsHeader);
          put(USER_ID_HEADER, expectedUserId);
        }});

      CompletableFuture<Event> future = eventPublisher.publish(eventPayload);

      String topicToObserve = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), tenant, DI_COMPLETED.value());
      DataImportEventPayload actualPayload = Json.decodeValue(getEventPayload(topicToObserve), DataImportEventPayload.class);
      assertEquals(eventPayload, actualPayload);
      assertEquals(expectedPermissionsHeader, actualPayload.getContext().get(PERMISSIONS_HEADER));
      assertEquals(expectedUserId, actualPayload.getContext().get(USER_ID_HEADER));
      assertEquals(expectedRecordId, actualPayload.getContext().get(RECORD_ID_HEADER));
      assertEquals(expectedChunkId, actualPayload.getContext().get(CHUNK_ID_HEADER));

      assertFalse(future.isCompletedExceptionally());
    }
  }

  @Test
  public void shouldPublishPayloadIfTokenIsNull() throws Exception {
    var tenant = "shouldPublishPayloadIfTokenIsNull";
    try(KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      DataImportEventPayload eventPayload = new DataImportEventPayload()
        .withEventType(DI_COMPLETED.value())
        .withOkapiUrl(OKAPI_URL)
        .withTenant(tenant)
        .withToken(null)
        .withContext(new HashMap<>() {{
          put("recordId", UUID.randomUUID().toString());
          put("chunkId", UUID.randomUUID().toString());
          put("userId", UUID.randomUUID().toString());
        }});

      CompletableFuture<Event> future = eventPublisher.publish(eventPayload);

      String topicToObserve = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), tenant, DI_COMPLETED.value());
      DataImportEventPayload actualPayload = Json.decodeValue(getEventPayload(topicToObserve), DataImportEventPayload.class);
      assertEquals(eventPayload, actualPayload);

      assertFalse(future.isCompletedExceptionally());
    }
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenPayloadIsNull() throws Exception {
    try(KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      CompletableFuture<Event> future = eventPublisher.publish(null);
      assertTrue(future.isCompletedExceptionally());
      future.get();
    }
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenPayloadParameterIsNull() throws Exception {
    try(KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      DataImportEventPayload eventPayload = new DataImportEventPayload()
        .withEventType(DI_COMPLETED.value())
        .withToken(TOKEN)
        .withOkapiUrl(OKAPI_URL)
        .withTenant(null)
        .withContext(new HashMap<>() {{
          put("recordId", UUID.randomUUID().toString());
        }});

      CompletableFuture<Event> future = eventPublisher.publish(eventPayload);
      assertTrue(future.isCompletedExceptionally());
      future.get();
    }
  }

  @Test
  public void shouldReturnFailedFutureWhenRecordIdIsNull() throws Exception {
    try(KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      DataImportEventPayload eventPayload = new DataImportEventPayload()
        .withEventType(DI_COMPLETED.value())
        .withOkapiUrl(OKAPI_URL)
        .withTenant(TENANT_ID)
        .withToken(TOKEN)
        .withContext(new HashMap<>() {{
          put("chunkId", UUID.randomUUID().toString());
        }});

      CompletableFuture<Event> future = eventPublisher.publish(eventPayload);
      assertFalse(future.isCompletedExceptionally());
      future.get();
    }
  }

  @Test
  public void shouldReturnFailedFutureWhenChunkIdIsNull() throws Exception {
    try(KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      DataImportEventPayload eventPayload = new DataImportEventPayload()
        .withEventType(DI_COMPLETED.value())
        .withOkapiUrl(OKAPI_URL)
        .withTenant(TENANT_ID)
        .withToken(TOKEN)
        .withContext(new HashMap<>() {{
          put("recordId", UUID.randomUUID().toString());
        }});

      CompletableFuture<Event> future = eventPublisher.publish(eventPayload);
      assertFalse(future.isCompletedExceptionally());
      future.get();
    }
  }

  private String getEventPayload(String topicToObserve) {
    try (var kafkaConsumer = new KafkaConsumer<String, String>(consumerConfig)) {
      kafkaConsumer.subscribe(List.of(topicToObserve));
      var records = kafkaConsumer.poll(Duration.ofSeconds(30));
      if (records.isEmpty()) {
        throw new IllegalStateException("Expected Kafka event at " + topicToObserve + " but got none");
      }
      Event obtainedEvent = Json.decodeValue(records.iterator().next().value(), Event.class);
      return obtainedEvent.getEventPayload();
    }
  }
}
