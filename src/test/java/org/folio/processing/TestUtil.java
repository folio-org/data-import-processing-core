package org.folio.processing;

import org.apache.commons.io.FileUtils;
import org.testcontainers.utility.DockerImageName;
import java.io.File;
import java.io.IOException;

/**
 * Util class contains helper methods for unit testing needs
 */
public final class TestUtil {

  public static final DockerImageName KAFKA_CONTAINER_NAME = DockerImageName.parse("apache/kafka-native:3.8.0");

  public static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }
}
