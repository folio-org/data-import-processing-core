package org.folio.processing.core.model;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The event context holds an information about event
 */
public class EventContext {
  private boolean handled;
  private String eventType;
  private Object profileSnapshot;
  private Object currentNode;
  private List<String> currentNodePath = new ArrayList<>();
  private List<String> eventChain = new LinkedList<>();
  private Map<String, String> objects;

  public EventContext() {
  }

  public boolean isHandled() {
    return handled;
  }

  public void setHandled(boolean handled) {
    this.handled = handled;
  }

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public Object getProfileSnapshot() {
    return profileSnapshot;
  }

  public void setProfileSnapshot(Object profileSnapshot) {
    this.profileSnapshot = profileSnapshot;
  }

  public Object getCurrentNode() {
    return currentNode;
  }

  public void setCurrentNode(Object currentNode) {
    this.currentNode = currentNode;
  }

  public List<String> getCurrentNodePath() {
    return currentNodePath;
  }

  public void setCurrentNodePath(List<String> currentNodePath) {
    this.currentNodePath = currentNodePath;
  }

  public List<String> getEventChain() {
    return eventChain;
  }

  public void setEventChain(List<String> eventChain) {
    this.eventChain = eventChain;
  }

  @Override
  public String toString() {
    return "EventContext{" +
      "eventType='" + eventType + '\'' +
      ", profileSnapshot=" + profileSnapshot +
      ", currentNode=" + currentNode +
      ", currentNodePath=" + currentNodePath +
      ", eventChain=" + eventChain +
      ", objects=" + objects +
      '}';
  }
}
