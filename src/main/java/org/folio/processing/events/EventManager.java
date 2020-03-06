package org.folio.processing.events;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.events.services.processor.EventProcessor;
import org.folio.processing.events.services.processor.EventProcessorImpl;
import org.folio.processing.events.services.publisher.EventPublisher;
import org.folio.processing.events.services.publisher.RestEventPublisher;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;

/**
 * The central class to use for handlers registration and event handling.
 */
public final class EventManager {

  private static final EventProcessor eventProcessor = new EventProcessorImpl();
  private static final EventPublisher eventPublisher = new RestEventPublisher();

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
          eventPublisher.publish(prepareEventPayload(eventPayload, processThrowable))
            .whenComplete((publishPayload, publishThrowable) -> {
              if (publishThrowable == null) {
                future.complete(eventPayload);
              } else {
                future.completeExceptionally(publishThrowable);
              }
            }));
    } catch (Exception e) {
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

  /**
   * Prepares given eventPayload for publishing.
   *
   * @param eventPayload eventPayload
   */
  private static DataImportEventPayload prepareEventPayload(DataImportEventPayload eventPayload, Throwable throwable) {
    if (throwable != null) {
      return prepareErrorEventPayload(eventPayload, throwable);
    }
    eventPayload.getCurrentNodePath().add(eventPayload.getCurrentNode().getId());
    List<ProfileSnapshotWrapper> children = eventPayload.getCurrentNode().getChildSnapshotWrappers();
    if (isNotEmpty(children)) {
      eventPayload.setCurrentNode(children.get(0));
    } else {
      ProfileSnapshotWrapper next = findNext(eventPayload.getProfileSnapshot(), eventPayload.getCurrentNode().getId());
      if (next != null) {
        eventPayload.setCurrentNode(next);
      } else {
        eventPayload.getEventsChain().add(eventPayload.getEventType());
        eventPayload.setEventType(DI_COMPLETED.value());
      }
    }
    return eventPayload;
  }

  // TODO current implementation is excessive and does not cover skipping of profiles based on match/non-match criterion
  // will be changed in scope of {@link https://issues.folio.org/browse/MODDICORE-33}
  private static ProfileSnapshotWrapper findNext(ProfileSnapshotWrapper root, String lastProcessedProfileId) {
    List<ProfileSnapshotWrapper> sequenceOfProfiles = buildSequenceOfProfiles(root);
    OptionalInt indexOfLastProcessed = IntStream.range(0, sequenceOfProfiles.size())
      .filter(i -> sequenceOfProfiles.get(i).getId().equals(lastProcessedProfileId))
      .findFirst();

    if (indexOfLastProcessed.isPresent() && indexOfLastProcessed.getAsInt() < sequenceOfProfiles.size() - 1) {
      return sequenceOfProfiles.get(indexOfLastProcessed.getAsInt());
    }
    return null;
  }

  private static List<ProfileSnapshotWrapper> buildSequenceOfProfiles(ProfileSnapshotWrapper root) {
    List<ProfileSnapshotWrapper> visitedNodes = new ArrayList<>();
    List<ProfileSnapshotWrapper> unvisitedNodes = new ArrayList<>();
    unvisitedNodes.add(root);
    while (!unvisitedNodes.isEmpty()) {
      ProfileSnapshotWrapper currentNode = unvisitedNodes.remove(0);
      List<ProfileSnapshotWrapper> newNodes = currentNode.getChildSnapshotWrappers()
        .stream()
        .filter(node -> !visitedNodes.contains(node))
        .collect(Collectors.toList());
      unvisitedNodes.addAll(0, newNodes);
      visitedNodes.add(currentNode);
    }
    return visitedNodes;
  }

  private static DataImportEventPayload prepareErrorEventPayload(DataImportEventPayload eventPayload, Throwable throwable) {
    eventPayload.setEventType(DI_ERROR.value());
    // an error occurred during handling of current event type, so it is pushed to the events chain
    eventPayload.getEventsChain().add(eventPayload.getEventType());
    eventPayload.getContext().put("ERROR", throwable.getMessage());
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
   * Clears the registry of event handlers.
   */
  public static void clearEventHandlers() {
    eventProcessor.getEventHandlers().clear();
  }
}
