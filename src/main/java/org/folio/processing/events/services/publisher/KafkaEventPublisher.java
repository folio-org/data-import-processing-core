package org.folio.processing.events.services.publisher;

import io.vertx.core.Future;
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
import org.folio.kafka.KafkaProducerManager;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SimpleKafkaProducerManager;
import org.folio.kafka.services.KafkaProducerRecordBuilder;
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
  private final KafkaProducerManager producerManager;

  public KafkaEventPublisher(KafkaConfig kafkaConfig, Vertx vertx, int maxDistributionNum) {
    this.kafkaConfig = kafkaConfig;
    this.vertx = vertx;
    this.maxDistributionNum = maxDistributionNum;
    this.producerManager = new SimpleKafkaProducerManager(vertx, kafkaConfig);
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

      String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
        eventPayload.getTenant(), eventType);

      var record = buildRecord(eventPayload, event, topicName);
      record.addHeaders(getHeaders(eventPayload, recordId, chunkId, jobExecutionId));

      KafkaProducer<String, String> producer = producerManager.createShared(eventType);
      producer.send(record)
        .<Void>mapEmpty()
        .eventually(() -> {
          Vertx.currentContext().owner()
            .setTimer(3000, t -> producer.flush().eventually(() -> producer.close()));
          return Future.succeededFuture();
        })
        .onSuccess(ar -> {
          LOGGER.info("publish:: Event with type: '{}' by jobExecutionId: '{}' and recordId: '{}' with chunkId: '{}' was sent to the topic '{}' ",
            eventType, jobExecutionId, recordId, chunkId, topicName);
          future.complete(event);
        })
        .onFailure(error -> {
          LOGGER.warn("publish:: {} send error for event: '{}' by jobExecutionId: '{}' with recordId: '{}' and with chunkId: '{}' ",
            eventType + "_Producer", eventType, jobExecutionId, recordId, chunkId, error);
          future.completeExceptionally(error);
        });
    } catch (Exception e) {
      LOGGER.warn("publish:: Can not publish event: {} with recordId: {}", eventType, recordId, e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private KafkaProducerRecord<String, String> buildRecord(DataImportEventPayload eventPayload, Event event, String topicName) {
    String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);
    return new KafkaProducerRecordBuilder<String, Object>(eventPayload.getTenant())
      .key(key)
      .value(event)
      .topic(topicName)
      .build();
  }

  private List<KafkaHeader> getHeaders(DataImportEventPayload eventPayload, String recordId, String chunkId, String jobExecutionId) {
    List<KafkaHeader> headers = new ArrayList<>();
    headers.add(KafkaHeader.header(OKAPI_URL_HEADER, eventPayload.getOkapiUrl()));
    headers.add(KafkaHeader.header(OKAPI_TENANT_HEADER, eventPayload.getTenant()));
    headers.add(KafkaHeader.header(OKAPI_TOKEN_HEADER, eventPayload.getToken()));
    checkAndAddHeaders(recordId, chunkId, jobExecutionId, headers);
    return headers;
  }

  private void checkAndAddHeaders(String recordId, String chunkId, String jobExecutionId, List<KafkaHeader> headers) {
    if (StringUtils.isBlank(recordId)) {
      LOGGER.warn("checkAndAddHeaders:: RecordId is empty for jobExecutionId: '{}' ", jobExecutionId);
    } else {
      headers.add(KafkaHeader.header(RECORD_ID_HEADER, recordId));
    }
    if (StringUtils.isBlank(chunkId)) {
      LOGGER.warn("checkAndAddHeaders:: ChunkId is empty for jobExecutionId: '{}' ", jobExecutionId);
    } else {
      headers.add(KafkaHeader.header(CHUNK_ID_HEADER, chunkId));
    }
  }
}
