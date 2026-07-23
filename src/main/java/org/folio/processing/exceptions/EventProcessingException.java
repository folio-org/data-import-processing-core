package org.folio.processing.exceptions;

public class EventProcessingException extends RuntimeException {

  public EventProcessingException(Throwable cause) {
    super(cause);
  }

  public EventProcessingException(String message) {
    super(message);
  }

  public EventProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}
