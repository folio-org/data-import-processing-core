package org.folio.processing.events.model;

import org.folio.ProfileSnapshotWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * The event context holds an information about event
 */
public class EventContext {
  private boolean handled;
  private String eventType;
  private ProfileSnapshotWrapper profileSnapshot;
  private ProfileSnapshotWrapper currentNode;
  private List<String> currentNodePath = new ArrayList<>();
  private List<String> eventChain = new LinkedList<>();
  private OkapiConnectionParams okapiConnectionParams;
  private Map<String, String> objects = new HashMap<>();

  public EventContext() {
  }

  public EventContext(String eventType, OkapiConnectionParams okapiConnectionParams) {
    this.eventType = eventType;
    this.okapiConnectionParams = okapiConnectionParams;
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

  public ProfileSnapshotWrapper getProfileSnapshot() {
    return profileSnapshot;
  }

  public void setProfileSnapshot(ProfileSnapshotWrapper profileSnapshot) {
    this.profileSnapshot = profileSnapshot;
  }

  public ProfileSnapshotWrapper getCurrentNode() {
    return currentNode;
  }

  public void setCurrentNode(ProfileSnapshotWrapper currentNode) {
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

  public OkapiConnectionParams getOkapiConnectionParams() {
    return okapiConnectionParams;
  }

  public Map<String, String> getObjects() {
    return objects;
  }

  public void setObjects(Map<String, String> objects) {
    this.objects = objects;
  }

  public String putObject(String key, String object) {
    return this.objects.put(key, object);
  }
}
