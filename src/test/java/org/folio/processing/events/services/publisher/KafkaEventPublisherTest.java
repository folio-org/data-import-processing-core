package org.folio.processing.events.services.publisher;

import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.processing.events.services.publisher.KafkaEventPublisher.CHUNK_ID_HEADER;
import static org.folio.processing.events.services.publisher.KafkaEventPublisher.PERMISSIONS_HEADER;
import static org.folio.processing.events.services.publisher.KafkaEventPublisher.RECORD_ID_HEADER;
import static org.folio.processing.events.services.publisher.KafkaEventPublisher.REQUEST_ID_HEADER;
import static org.folio.processing.events.services.publisher.KafkaEventPublisher.USER_ID_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.TestUtil;
import org.folio.rest.jaxrs.model.Event;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

class KafkaEventPublisherTest {
  static KafkaContainer kafkaContainer = new KafkaContainer(TestUtil.KAFKA_CONTAINER_NAME);
  private static final String KAFKA_ENV = "folio";
  private static final String OKAPI_URL = "http://localhost";
  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "stub-token";
  private static final Properties CONSUMER_CONFIG = new Properties();
  private static KafkaConfig kafkaConfig;
  private final Vertx vertx = Vertx.vertx();

  @BeforeAll
  static void setUpClass() {
    kafkaContainer.start();
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(kafkaContainer.getHost())
      .kafkaPort(kafkaContainer.getFirstMappedPort() + "")
      .envId(KAFKA_ENV)
      .build();
    kafkaConfig.getConsumerProps().forEach((key, value) -> {
      if (value != null) {
        CONSUMER_CONFIG.put(key, value);
      }
    });
    CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
  }

  @AfterAll
  static void tearDownClass() {
    kafkaContainer.stop();
  }

  @Test
  void shouldPublishPayload() throws Exception {
    var tenant = "shouldPublishPayload";
    String expectedPermissionsHeader = JsonArray.of("test-permission").encode();
    String expectedUserId = UUID.randomUUID().toString();
    String expectedRecordId = UUID.randomUUID().toString();
    String expectedChunkId = UUID.randomUUID().toString();
    String expectedRequestId = UUID.randomUUID().toString();
    try (KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      DataImportEventPayload eventPayload = new DataImportEventPayload()
        .withEventType(DI_COMPLETED.value())
        .withOkapiUrl(OKAPI_URL)
        .withTenant(tenant)
        .withToken(TOKEN)
        .withContext(contextOf(
          RECORD_ID_HEADER, expectedRecordId,
          CHUNK_ID_HEADER, expectedChunkId,
          PERMISSIONS_HEADER, expectedPermissionsHeader,
          USER_ID_HEADER, expectedUserId,
          REQUEST_ID_HEADER, expectedRequestId));

      CompletableFuture<Event> future = eventPublisher.publish(eventPayload);
      assertFalse(future.isCompletedExceptionally());

      String topicToObserve =
        KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), tenant, DI_COMPLETED.value());
      DataImportEventPayload actualPayload =
        Json.decodeValue(getEventPayload(topicToObserve), DataImportEventPayload.class);
      assertEquals(eventPayload, actualPayload);
      assertEquals(expectedPermissionsHeader, actualPayload.getContext().get(PERMISSIONS_HEADER));
      assertEquals(expectedUserId, actualPayload.getContext().get(USER_ID_HEADER));
      assertEquals(expectedRecordId, actualPayload.getContext().get(RECORD_ID_HEADER));
      assertEquals(expectedChunkId, actualPayload.getContext().get(CHUNK_ID_HEADER));
      assertEquals(expectedRequestId, actualPayload.getContext().get(REQUEST_ID_HEADER));
    }
  }

  @Test
  void shouldPublishPayloadIfTokenIsNull() throws Exception {
    var tenant = "shouldPublishPayloadIfTokenIsNull";
    try (KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      DataImportEventPayload eventPayload = new DataImportEventPayload()
        .withEventType(DI_COMPLETED.value())
        .withOkapiUrl(OKAPI_URL)
        .withTenant(tenant)
        .withToken(null)
        .withContext(contextOf(
          "recordId", UUID.randomUUID().toString(),
          "chunkId", UUID.randomUUID().toString(),
          "userId", UUID.randomUUID().toString()));

      CompletableFuture<Event> future = eventPublisher.publish(eventPayload);
      assertFalse(future.isCompletedExceptionally());

      String topicToObserve =
        KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), tenant, DI_COMPLETED.value());
      DataImportEventPayload actualPayload =
        Json.decodeValue(getEventPayload(topicToObserve), DataImportEventPayload.class);
      assertEquals(eventPayload, actualPayload);
    }
  }

  @Test
  void shouldReturnFailedFutureWhenPayloadIsNull() throws Exception {
    try (KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      CompletableFuture<Event> future = eventPublisher.publish(null);
      assertTrue(future.isCompletedExceptionally());
      assertThrows(ExecutionException.class, future::get);
    }
  }

  @Test
  void shouldReturnFailedFutureWhenPayloadParameterIsNull() throws Exception {
    try (KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      DataImportEventPayload eventPayload = new DataImportEventPayload()
        .withEventType(DI_COMPLETED.value())
        .withToken(TOKEN)
        .withOkapiUrl(OKAPI_URL)
        .withTenant(null)
        .withContext(contextOf("recordId", UUID.randomUUID().toString()));

      CompletableFuture<Event> future = eventPublisher.publish(eventPayload);
      assertTrue(future.isCompletedExceptionally());
      assertThrows(ExecutionException.class, future::get);
    }
  }

  @Test
  void shouldReturnFailedFutureWhenRecordIdIsNull() throws Exception {
    try (KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      DataImportEventPayload eventPayload = new DataImportEventPayload()
        .withEventType(DI_COMPLETED.value())
        .withOkapiUrl(OKAPI_URL)
        .withTenant(TENANT_ID)
        .withToken(TOKEN)
        .withContext(contextOf("chunkId", UUID.randomUUID().toString()));

      CompletableFuture<Event> future = eventPublisher.publish(eventPayload);
      assertFalse(future.isCompletedExceptionally());
      future.get();
    }
  }

  @Test
  void shouldReturnFailedFutureWhenChunkIdIsNull() throws Exception {
    try (KafkaEventPublisher eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      DataImportEventPayload eventPayload = new DataImportEventPayload()
        .withEventType(DI_COMPLETED.value())
        .withOkapiUrl(OKAPI_URL)
        .withTenant(TENANT_ID)
        .withToken(TOKEN)
        .withContext(contextOf("recordId", UUID.randomUUID().toString()));

      CompletableFuture<Event> future = eventPublisher.publish(eventPayload);
      assertFalse(future.isCompletedExceptionally());
      future.get();
    }
  }

  private String getEventPayload(String topicToObserve) {
    try (var kafkaConsumer = new KafkaConsumer<String, String>(CONSUMER_CONFIG)) {
      kafkaConsumer.subscribe(List.of(topicToObserve));
      var records = kafkaConsumer.poll(Duration.ofSeconds(30));
      if (records.isEmpty()) {
        throw new IllegalStateException("Expected Kafka event at " + topicToObserve + " but got none");
      }
      Event obtainedEvent = Json.decodeValue(records.iterator().next().value(), Event.class);
      return obtainedEvent.getEventPayload();
    }
  }

  private static HashMap<String, String> contextOf(String... entries) {
    HashMap<String, String> context = new HashMap<>();
    for (int i = 0; i < entries.length; i += 2) {
      context.put(entries[i], entries[i + 1]);
    }
    return context;
  }
}
