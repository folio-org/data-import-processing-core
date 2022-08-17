package org.folio.processing.mapping.defaultmapper.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class EscaperTest {
  private static final String WITHOUT_TRAILING_BACKSLASH = "test";
  private static final String WITH_TRAILING_BACKSLASH = "test\\";

  @Test
  void shouldRemoveTrailingBackslashByDefault() {
    var result = Escaper.escape(WITH_TRAILING_BACKSLASH);
    assertEquals(WITHOUT_TRAILING_BACKSLASH, result);
  }

  @Test
  void shouldNotRemoveTrailingBackslash() {
    var result = Escaper.escape(WITH_TRAILING_BACKSLASH, true);
    assertEquals(WITH_TRAILING_BACKSLASH, result);
  }

  @Test
  void shouldRemoveTrailingBackslash() {
    var result = Escaper.escape(WITH_TRAILING_BACKSLASH, false);
    assertEquals(WITHOUT_TRAILING_BACKSLASH, result);
  }
}
