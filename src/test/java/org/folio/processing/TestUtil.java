package org.folio.processing;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.testcontainers.utility.DockerImageName;

/**
 * Util class contains helper methods for unit testing needs
 */
public final class TestUtil {

  public static final DockerImageName KAFKA_CONTAINER_NAME = DockerImageName.parse("apache/kafka-native:4.2.0");

  public static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }
}
