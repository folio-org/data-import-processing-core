package org.folio.processing.exceptions;

public class MappingException extends RuntimeException {

  public MappingException(Throwable cause) {
    super(cause);
  }

  public MappingException(String message) {
    super(message);
  }

  public MappingException(String message, Throwable cause) {
    super(message, cause);
  }
}
