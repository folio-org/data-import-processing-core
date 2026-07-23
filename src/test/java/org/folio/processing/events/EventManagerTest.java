package org.folio.processing.events;

import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.TestUtil;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.kafka.KafkaContainer;

@ExtendWith(VertxExtension.class)
class EventManagerTest {
  private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(TestUtil.KAFKA_CONTAINER_NAME);
  private static final String KAFKA_ENV = "folio";
  private static KafkaConfig kafkaConfig;

  @BeforeAll
  static void setUpClass() {
    KAFKA_CONTAINER.start();
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(KAFKA_CONTAINER.getHost())
      .kafkaPort(KAFKA_CONTAINER.getFirstMappedPort() + "")
      .envId(KAFKA_ENV)
      .build();
  }

  @AfterAll
  static void tearDownClass() {
    KAFKA_CONTAINER.stop();
  }

  @BeforeEach
  void setUp() {
    EventManager.clearEventHandlers();
  }

  @Test
  void registerKafkaEventPublisher(Vertx vertx) {
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, 100);
    assertEquals(1, EventManager.getEventPublishers().size());
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, 100);
    assertEquals(1, EventManager.getEventPublishers().size());
  }

  @Test
  void shouldCompleteSuccessfullyIfNoEventHandlersFound(VertxTestContext testContext) {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_SRS_MARC_BIB_RECORD_CREATED")
      .withTenant("diku")
      .withOkapiUrl("http://localhost:9130")
      .withToken("token")
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(new HashMap<>());

    ProfileSnapshotWrapper child = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(UUID.randomUUID().toString())
      .withContentType(ACTION_PROFILE);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withChildSnapshotWrappers(Collections.singletonList(child));

    // when
    CompletableFuture<DataImportEventPayload> future = EventManager.handleEvent(eventPayload, profileSnapshotWrapper);

    // then
    future.whenComplete((payload, throwable) -> {
      testContext.verify(() -> {
        assertNull(throwable);
        assertNotNull(payload);
      });
      testContext.completeNow();
    });
  }
}
