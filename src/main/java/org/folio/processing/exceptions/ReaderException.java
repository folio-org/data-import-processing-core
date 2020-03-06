package org.folio.processing.exceptions;

public class ReaderException extends RuntimeException {

  public ReaderException(Throwable throwable) {
    super(throwable);
  }

  public ReaderException(String message) {
    super(message);
  }

  public ReaderException(String message, Throwable cause) {
    super(message, cause);
  }
}
