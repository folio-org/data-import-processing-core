package org.folio.processing.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProfileManager {
  private static final String PROFILE_PROPERTIES_FILE = "profile.properties";
  private static final String PROFILE_KEY = "profile.active";
  private static String activeProfile;

  static {
    loadProfile();
  }

  private static void loadProfile() {
    Properties properties = new Properties();
    try (InputStream inputStream = ProfileManager.class.getClassLoader().getResourceAsStream(PROFILE_PROPERTIES_FILE)) {
      if (inputStream != null) {
        properties.load(inputStream);
        activeProfile = properties.getProperty(PROFILE_KEY);
      } else {
        throw new IllegalStateException("Cannot find " + PROFILE_PROPERTIES_FILE + " in classpath");
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to load " + PROFILE_PROPERTIES_FILE, e);
    }
  }

  public static String getActiveProfile() {
    return activeProfile;
  }

  public static void reload() {
    loadProfile();
  }
}
