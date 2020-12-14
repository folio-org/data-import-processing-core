package org.folio.processing.events.services.publisher;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.util.pubsub.PubSubClientUtils;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class KafkaEventPublisher implements EventPublisher {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventPublisher.class);

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
    try {
      OkapiConnectionParams params = new OkapiConnectionParams();
      params.setOkapiUrl(eventPayload.getOkapiUrl());
      params.setTenantId(eventPayload.getTenant());
      params.setToken(eventPayload.getToken());
      Event event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventPayload.getEventType())
        .withEventPayload(ZIPArchiver.zip(JsonObject.mapFrom(eventPayload).encode()))
        .withEventMetadata(new EventMetadata()
          .withTenantId(params.getTenantId())
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.constructModuleName()));

      String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);

      String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
        eventPayload.getTenant(), eventType);

      KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create(topicName, key, Json.encode(event));

      record.addHeaders(params.getHeaders()
        .entrySet()
        .stream()
        .map(e -> KafkaHeader.header(e.getKey(), e.getValue()))
        .collect(Collectors.toList()));

      String producerName = eventType + "_Producer";
      KafkaProducer<String, String> producer =
        KafkaProducer.createShared(vertx, producerName, kafkaConfig.getProducerProps());

      producer.write(record, war -> {
        producer.end(ear -> producer.close());
        if (war.succeeded()) {
          LOGGER.info("Event with type {} was sent to the topic", eventType);
          future.complete(event);
        } else {
          Throwable cause = war.cause();
          LOGGER.error("{} write error for event {}:", cause, producerName, eventType);
          future.completeExceptionally(cause);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Can not publish event {}", e, eventType);
      future.completeExceptionally(e);
    }
    return future;
  }
}
