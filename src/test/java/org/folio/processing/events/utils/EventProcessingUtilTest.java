package org.folio.processing.events.utils;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class EventProcessingUtilTest {

  @Test
  public void shouldConstructValiModuleNameWithVersion(){
    String expectedModuleName = "data-import-processing-core";
    String actualModuleNameWithVersion = EventProcessingUtil.getModuleName();
    assertNotNull(actualModuleNameWithVersion);
    assertTrue(actualModuleNameWithVersion.contains(expectedModuleName));
    assertFalse(actualModuleNameWithVersion.contains("SNAPSHOT"));
    assertFalse(actualModuleNameWithVersion.contains("_"));
    assertTrue(actualModuleNameWithVersion.contains("."));
  }
}
