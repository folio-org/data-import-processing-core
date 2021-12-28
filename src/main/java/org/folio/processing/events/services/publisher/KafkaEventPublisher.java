package org.folio.processing.events.services.publisher;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.PomReaderUtil;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

public class KafkaEventPublisher implements EventPublisher {
  private static final Logger LOGGER = LogManager.getLogger(KafkaEventPublisher.class);
  public static final String RECORD_ID_HEADER = "recordId";
  public static final String CHUNK_ID_HEADER = "chunkId";


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
    String recordId = eventPayload.getContext().get(RECORD_ID_HEADER);
    String chunkId = eventPayload.getContext().get(CHUNK_ID_HEADER);
    String jobExecutionId = eventPayload.getJobExecutionId();

    try {
      Event event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventPayload.getEventType())
        .withEventPayload(Json.encode(eventPayload))
        .withEventMetadata(new EventMetadata()
          .withTenantId(eventPayload.getTenant())
          .withEventTTL(1)
          .withPublishedBy(PomReaderUtil.INSTANCE.constructModuleVersionAndVersion(PomReaderUtil.INSTANCE.getModuleName(), PomReaderUtil.INSTANCE.getVersion())));

      String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);

      String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
        eventPayload.getTenant(), eventType);

      KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create(topicName, key, Json.encode(event));

      List<KafkaHeader> headers = new ArrayList<>();
      headers.add(KafkaHeader.header(OKAPI_URL_HEADER, eventPayload.getOkapiUrl()));
      headers.add(KafkaHeader.header(OKAPI_TENANT_HEADER, eventPayload.getTenant()));
      headers.add(KafkaHeader.header(OKAPI_TOKEN_HEADER, eventPayload.getToken()));
      checkAndAddHeaders(recordId, chunkId, jobExecutionId, headers);

      record.addHeaders(headers);

      String producerName = eventType + "_Producer";
      KafkaProducer<String, String> producer =
        KafkaProducer.createShared(vertx, producerName, kafkaConfig.getProducerProps());

      producer.write(record, war -> {
        producer.end(ear -> producer.close());
        if (war.succeeded()) {
          LOGGER.info("Event with type: '{}' by jobExecutionId: '{}' and recordId: '{}' with chunkId: '{}' was sent to the topic '{}' ", eventType, jobExecutionId, recordId, chunkId, topicName);
          future.complete(event);
        } else {
          Throwable cause = war.cause();
          LOGGER.error("{} write error for event: '{}' by jobExecutionId: '{}' with recordId: '{}' and with chunkId: '{}' ", producerName, jobExecutionId, eventType, recordId, chunkId, cause);
          future.completeExceptionally(cause);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Can not publish event: {} with recordId: {}", eventType, recordId, e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private void checkAndAddHeaders(String recordId, String chunkId, String jobExecutionId, List<KafkaHeader> headers) {
    if (StringUtils.isBlank(recordId)) {
      LOGGER.warn("RecordId is empty for jobExecutionId: '{}' ", jobExecutionId);
    } else {
      headers.add(KafkaHeader.header(RECORD_ID_HEADER, recordId));
    }
    if (StringUtils.isBlank(chunkId)) {
      LOGGER.warn("ChunkId is empty for jobExecutionId: '{}' ", jobExecutionId);
    } else {
      headers.add(KafkaHeader.header(CHUNK_ID_HEADER, chunkId));
    }
  }
}
