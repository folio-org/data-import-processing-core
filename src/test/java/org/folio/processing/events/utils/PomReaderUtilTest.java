package org.folio.processing.events.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.maven.model.Dependency;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PomReaderUtilTest {

  @AfterEach
  void tearDown() {
    PomReaderUtil pom = PomReaderUtil.INSTANCE;
    pom.init("pom.xml");  // restore for other unit tests (it's a singleton)
  }

  @Test
  void testGetModuleName() {
    assertEquals("data_import_processing_core", PomReaderUtil.INSTANCE.getModuleName());
  }

  @Test
  void testGetVersion() {
    assertTrue(PomReaderUtil.INSTANCE.getVersion().matches("[0-9]+\\.[0-9]+\\..*"));
  }

  @Test
  void testGetProps() {
    assertNull(PomReaderUtil.INSTANCE.getProps().getProperty("does_not_exist"));
  }

  @Test
  void testGetDependencies() {
    List<Dependency> dependencies = PomReaderUtil.INSTANCE.getDependencies();
    assertFalse(dependencies.isEmpty());
  }

  @Test
  void readFromJar() throws IOException, XmlPullParserException {
    PomReaderUtil pom = PomReaderUtil.INSTANCE;

    pom.readIt(null, "META-INF/maven/io.vertx");  // force reading from Jar
    // first dependency in main pom
    assertEquals("vertx_core_aggregator", pom.getModuleName());
  }

  @Test
  void readFromJarNoPom() {
    PomReaderUtil pom = PomReaderUtil.INSTANCE;

    assertThrows(NullPointerException.class, () -> pom.readIt(null, "ramls"));
  }

  @Test
  void readFromJarNoResource() {
    PomReaderUtil pom = PomReaderUtil.INSTANCE;

    assertThrows(NullPointerException.class, () -> pom.readIt(null, "pom/pom-sample.xml"));
  }

  @Test
  void shouldFailForBadFilename() {
    PomReaderUtil pom = PomReaderUtil.INSTANCE;

    assertThrows(IllegalArgumentException.class, () -> pom.init("does_not_exist.xml"));
  }

  @Test
  void otherPom() {
    PomReaderUtil pom = PomReaderUtil.INSTANCE;

    pom.init("src/test/resources/org/folio/processing/pom/pom-sample.xml");
    assertEquals("mod_inventory_storage", PomReaderUtil.INSTANCE.getModuleName());
    assertEquals("19.4.0", PomReaderUtil.INSTANCE.getVersion());
  }
}
