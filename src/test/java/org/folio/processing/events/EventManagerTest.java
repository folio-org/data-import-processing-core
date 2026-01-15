package org.folio.processing.events;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.TestUtil;
import org.folio.processing.events.services.publisher.KafkaEventPublisher;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.kafka.KafkaContainer;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;

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

  @Before
  public void setUp() {
    EventManager.clearEventHandlers();
  }

  @Test
  public void registerKafkaEventPublisher(TestContext context) {
    Vertx vertx = rule.vertx();
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, 100);
    context.assertEquals(1, EventManager.getEventPublishers().size());
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, 100);
    context.assertEquals(1, EventManager.getEventPublishers().size());
  }

  @Test
  public void shouldCompleteSuccessfullyIfNoEventHandlersFound(TestContext context) {
    Async async = context.async();
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
      context.assertNull(throwable);
      context.assertNotNull(payload);
      async.complete();
    });
  }

  @Test
  public void testConcurrentKafkaEventPublisherRegistration(TestContext context) throws InterruptedException {
    Async async = context.async();
    Vertx vertx = rule.vertx();
    int threadCount = 10;
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger errorCount = new AtomicInteger(0);

    // Spawn multiple threads that register publishers concurrently
    for (int i = 0; i < threadCount; i++) {
      new Thread(() -> {
        try {
          EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, 100);
        } catch (Exception e) {
          errorCount.incrementAndGet();
          context.fail("Exception occurred during concurrent registration: " + e.getMessage());
        } finally {
          latch.countDown();
        }
      }).start();
    }

    // Wait for all threads to complete
    boolean completed = latch.await(10, TimeUnit.SECONDS);
    context.assertTrue(completed, "All threads should complete within timeout");

    // Verify no errors occurred
    context.assertEquals(0, errorCount.get(), "Expected no concurrent modification errors");

    // Verify only 1 publisher is registered (last one wins)
    context.assertEquals(1, EventManager.getEventPublishers().size());

    // Verify the publisher is a KafkaEventPublisher
    context.assertTrue(EventManager.getEventPublishers().get(0) instanceof KafkaEventPublisher);

    async.complete();
  }
}
