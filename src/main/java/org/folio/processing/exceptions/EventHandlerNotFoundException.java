package org.folio.processing.exceptions;

public class EventHandlerNotFoundException extends EventProcessingException {

  public EventHandlerNotFoundException(String message) {
    super(message);
  }
}
