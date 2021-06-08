package org.folio.processing.events.services.publisher;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.util.pubsub.PubSubClientUtils;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

public class KafkaEventPublisher implements EventPublisher {
  private static final Logger LOGGER = LogManager.getLogger(KafkaEventPublisher.class);
  public static final String CORRELATION_ID_HEADER = "correlationId";

  private static final AtomicLong indexer = new AtomicLong();

  private final KafkaConfig kafkaConfig;
  private final Vertx vertx;
  private final Integer maxDistributionNum;

  public KafkaEventPublisher(KafkaConfig kafkaConfig, Vertx vertx, int maxDistributionNum) {
    this.kafkaConfig = kafkaConfig;
    this.vertx = vertx;
    this.maxDistributionNum = maxDistributionNum;
  }

  @Override
  public CompletableFuture<Event> publish(DataImportEventPayload eventPayload) {
    CompletableFuture<Event> future = new CompletableFuture<>();
    if (eventPayload == null) {
      future.completeExceptionally(new IllegalArgumentException("DataImportEventPayload can't be null"));
      return future;
    }

    String eventType = eventPayload.getEventType();
    String correlationId = eventPayload.getContext().get(CORRELATION_ID_HEADER) != null
      ? eventPayload.getContext().get(CORRELATION_ID_HEADER) : UUID.randomUUID().toString();

    try {
      Event event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventPayload.getEventType())
        .withEventPayload(ZIPArchiver.zip(JsonObject.mapFrom(eventPayload).encode()))
        .withEventMetadata(new EventMetadata()
          .withTenantId(eventPayload.getTenant())
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.getModuleId()));

      String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);

      String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
        eventPayload.getTenant(), eventType);

      KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create(topicName, key, Json.encode(event));

      record.addHeaders(List.of(
        KafkaHeader.header(OKAPI_URL_HEADER, eventPayload.getOkapiUrl()),
        KafkaHeader.header(OKAPI_TENANT_HEADER, eventPayload.getTenant()),
        KafkaHeader.header(OKAPI_TOKEN_HEADER, eventPayload.getToken()),
        KafkaHeader.header(CORRELATION_ID_HEADER, correlationId)));

      String producerName = eventType + "_Producer";
      KafkaProducer<String, String> producer =
        KafkaProducer.createShared(vertx, producerName, kafkaConfig.getProducerProps());

      producer.write(record, war -> {
        producer.end(ear -> producer.close());
        if (war.succeeded()) {
          LOGGER.info("Event with type: {} and correlationId: {} was sent to the topic {}", eventType, correlationId, topicName);
          future.complete(event);
        } else {
          Throwable cause = war.cause();
          LOGGER.error("{} write error for event: {} with correlationId: {}", producerName, eventType, correlationId, cause);
          future.completeExceptionally(cause);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Can not publish event: {} with correlationId: {}", eventType, correlationId, e);
      future.completeExceptionally(e);
    }
    return future;
  }
}
