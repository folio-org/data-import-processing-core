package org.folio.processing.events;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.events.services.processor.EventProcessor;
import org.folio.processing.events.services.processor.EventProcessorImpl;
import org.folio.processing.events.services.publisher.EventPublisher;
import org.folio.processing.events.services.publisher.KafkaEventPublisher;
import org.folio.processing.events.services.publisher.RestEventPublisher;
import org.folio.processing.exceptions.EventHandlerNotFoundException;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.lang.Boolean.parseBoolean;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;

/**
 * The central class to use for handlers registration and event handling.
 */
public final class EventManager {

  public static final String POST_PROCESSING_INDICATOR = "POST_PROCESSING";
  public static final String POST_PROCESSING_RESULT_EVENT_KEY = "POST_PROCESSING_RESULT_EVENT";

  private static final Logger LOGGER = LoggerFactory.getLogger(EventManager.class);

  private static final EventProcessor eventProcessor = new EventProcessorImpl();
  private static final List<EventPublisher> eventPublisher = new ArrayList<>(Arrays.<EventPublisher>asList(new RestEventPublisher()));


  private EventManager() {
  }

  /**
   * Handles the given payload of event.
   * If there are handlers found to handle event then the EventManager calls EventProcessor passing event payload.
   * After processing the EventManager calls EventPublisher to send next event up to the queue.
   *
   * @param eventPayload event payload
   * @return future with event payload after handling
   */
  public static CompletableFuture<DataImportEventPayload> handleEvent(DataImportEventPayload eventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      setCurrentNodeIfRoot(eventPayload);
      eventProcessor.process(eventPayload)
        .whenComplete((processPayload, processThrowable) ->
          publishEventIfNecessary(eventPayload, processThrowable)
            .whenComplete((publishPayload, publishThrowable) -> {
              if (publishThrowable == null) {
                future.complete(eventPayload);
              } else {
                LOGGER.error("Can`t publish event", publishThrowable);
                future.completeExceptionally(publishThrowable);
              }
            }));
    } catch (Exception e) {
      LOGGER.error("Can`t handle event", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private static void setCurrentNodeIfRoot(DataImportEventPayload eventPayload) {
    if (eventPayload.getCurrentNode() == null || eventPayload.getCurrentNode().getContentType() == JOB_PROFILE) {
      List<ProfileSnapshotWrapper> jobProfileChildren = eventPayload.getProfileSnapshot().getChildSnapshotWrappers();
      if (isNotEmpty(jobProfileChildren)) {
        eventPayload.setCurrentNode(jobProfileChildren.get(0));
      }
      eventPayload.setCurrentNodePath(new ArrayList<>(Collections.singletonList(eventPayload.getProfileSnapshot().getId())));
    }
  }

  private static CompletableFuture<Boolean> publishEventIfNecessary(DataImportEventPayload eventPayload, Throwable processThrowable) {
    if (processThrowable instanceof EventHandlerNotFoundException) {
      return CompletableFuture.completedFuture(false);
    }
    return eventPublisher.get(0).publish(prepareEventPayload(eventPayload, processThrowable))
      .thenApply(sentEvent -> true);
  }

  /**
   * Prepares given eventPayload for publishing.
   *
   * @param eventPayload eventPayload
   */
  private static DataImportEventPayload prepareEventPayload(DataImportEventPayload eventPayload, Throwable throwable) {
    if (throwable != null) {
      return prepareErrorEventPayload(eventPayload, throwable);
    }
    if (parseBoolean(eventPayload.getContext().get(POST_PROCESSING_INDICATOR))) {
      eventPayload.getContext().remove(POST_PROCESSING_INDICATOR);
      return eventPayload;
    }
    if (isNotBlank(eventPayload.getContext().get(POST_PROCESSING_RESULT_EVENT_KEY))) {
      eventPayload.setEventType(eventPayload.getContext().remove(POST_PROCESSING_RESULT_EVENT_KEY));
    }

    eventPayload.getCurrentNodePath().add(eventPayload.getCurrentNode().getId());
    Optional<ProfileSnapshotWrapper> next = findNext(eventPayload);
    if (next.isPresent()) {
      eventPayload.setCurrentNode(next.get());
    } else {
      eventPayload.getEventsChain().add(eventPayload.getEventType());
      eventPayload.setEventType(DI_COMPLETED.value());
    }
    return eventPayload;
  }

  private static Optional<ProfileSnapshotWrapper> findNext(DataImportEventPayload eventPayload) {
    String eventType = eventPayload.getEventType();
    ProfileSnapshotWrapper currentNode = eventPayload.getCurrentNode();
    if (currentNode.getContentType() == MATCH_PROFILE) {
      ProfileSnapshotWrapper.ReactTo targetReactTo = eventType.endsWith("NOT_MATCHED")
        ? ProfileSnapshotWrapper.ReactTo.NON_MATCH
        : ProfileSnapshotWrapper.ReactTo.MATCH;

      Optional<ProfileSnapshotWrapper> optionalNext = currentNode.getChildSnapshotWrappers()
        .stream()
        .filter(child -> targetReactTo == child.getReactTo())
        .findFirst();
      if (optionalNext.isPresent()) {
        return optionalNext;
      } else {
        return findParent(currentNode.getId(), eventPayload.getProfileSnapshot())
          .flatMap(matchParent -> getNextChildProfile(currentNode, matchParent));
      }
    }
    if (currentNode.getContentType() == MAPPING_PROFILE) {
      return findParent(currentNode.getId(), eventPayload.getProfileSnapshot())
        .flatMap(mappingParent -> findParent(mappingParent.getId(), eventPayload.getProfileSnapshot())
          .flatMap(actionParent -> getNextChildProfile(mappingParent, actionParent)
            .or(() -> findParent(actionParent.getId(), eventPayload.getProfileSnapshot())
              .flatMap(matchParent -> getNextChildProfile(actionParent, matchParent)))));
    }
    if (currentNode.getContentType() == ACTION_PROFILE) {
      if (isNotEmpty(currentNode.getChildSnapshotWrappers())) {
        return Optional.of(currentNode.getChildSnapshotWrappers().get(0));
      } else {
        return findParent(currentNode.getId(), eventPayload.getProfileSnapshot())
          .flatMap(actionParent -> getNextChildProfile(currentNode, actionParent));
      }
    }
    return Optional.empty();
  }

  private static Optional<ProfileSnapshotWrapper> findParent(String currentWrapperId, ProfileSnapshotWrapper root) {
    for (ProfileSnapshotWrapper childWrapper : root.getChildSnapshotWrappers()) {
      if (childWrapper.getId().equals(currentWrapperId)) {
        return Optional.of(root);
      }
      Optional<ProfileSnapshotWrapper> parent = findParent(currentWrapperId, childWrapper);
      if (parent.isPresent()) {
        return parent;
      }
    }
    return Optional.empty();
  }

  private static Optional<ProfileSnapshotWrapper> getNextChildProfile(ProfileSnapshotWrapper currentChild, ProfileSnapshotWrapper parent) {
    return parent.getChildSnapshotWrappers()
      .stream()
      .sorted(Comparator.comparing(ProfileSnapshotWrapper::getOrder))
      .filter(child -> child.getReactTo() == currentChild.getReactTo() && child.getOrder() > currentChild.getOrder())
      .findFirst();
  }

  private static DataImportEventPayload prepareErrorEventPayload(DataImportEventPayload eventPayload, Throwable throwable) {
    // an error occurred during handling of current event type, so it is pushed to the events chain
    eventPayload.getEventsChain().add(eventPayload.getEventType());
    eventPayload.getContext().put("ERROR", throwable.getMessage());
    eventPayload.setEventType(DI_ERROR.value());
    return eventPayload;
  }

  /**
   * Performs registration for given event handler in processing list
   *
   * @param eventHandler event handler
   * @return true handlers is registered
   */
  public static <T extends EventHandler> boolean registerEventHandler(T eventHandler) {
    return eventProcessor.getEventHandlers().add(eventHandler);
  }

  /**
   * Performs registration for kafka event publisher in publishers list
   *
   * @param kafkaConfig - object with kafka initial params
   * @param vertx       - vertx instance
   */
  public static void registerKafkaEventPublisher(KafkaConfig kafkaConfig, Vertx vertx, int maxDistributionNum) {
    eventPublisher.clear();
    eventPublisher.add(new KafkaEventPublisher(kafkaConfig, vertx, maxDistributionNum));
  }

  /**
   * Performs registration for rest event publisher in publishers list
   */
  public static void registerRestEventPublisher() {
    eventPublisher.clear();
    eventPublisher.add(new RestEventPublisher());
  }

  /**
   * Clears the registry of event handlers.
   */
  public static void clearEventHandlers() {
    eventProcessor.getEventHandlers().clear();
  }
}
