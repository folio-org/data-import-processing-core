package org.folio.processing.events.utils;

import java.io.FileReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.folio.processing.exceptions.EventProcessingException;

/**
 * Util which helps process events.
 */
public final class EventProcessingUtil {

  private static final Logger LOG = LogManager.getLogger(EventProcessingUtil.class);
  private static final String POM_FILE = "pom.xml";

  private EventProcessingUtil() {
  }

  /**
   * Retrieves module name with version.
   * Example: "data-import-processing-core-3.1.0".
   * Note: it should be without "_" and "SNAPSHOT".
   * @return = actual module name with module version
   */
  public static String getModuleName() {
    try {
      var reader = new MavenXpp3Reader();
      var model = reader.read(new FileReader(POM_FILE));
      return model.getArtifactId() + "-" + model.getVersion().replaceAll("-.*", "");
    } catch (Exception e) {
      String errorMessage = "Can't construct module name: " + e;
      LOG.error(errorMessage);
      throw new EventProcessingException(errorMessage);
    }
  }
}
