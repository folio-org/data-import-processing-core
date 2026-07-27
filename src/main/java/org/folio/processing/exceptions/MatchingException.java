package org.folio.processing.exceptions;

public class MatchingException extends RuntimeException {

  public MatchingException(Throwable cause) {
    super(cause);
  }

  public MatchingException(String message) {
    super(message);
  }

  public MatchingException(String message, Throwable cause) {
    super(message, cause);
  }
}

