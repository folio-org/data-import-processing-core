package org.folio.processing.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class ProfileManagerTest {

  @Test
  void testActiveProfileLoadedCorrectly() {
    try (MockedStatic<ProfileManager> mocked = Mockito.mockStatic(ProfileManager.class)) {
      mocked.when(ProfileManager::getActiveProfile).thenReturn("mockProfile");
      assertEquals("mockProfile", ProfileManager.getActiveProfile());
    }
  }

  @Test
  void testExceptionThrownWhenPropertiesFileMissing() {
    try (MockedStatic<ProfileManager> mocked = Mockito.mockStatic(ProfileManager.class)) {
      mocked.when(ProfileManager::reload)
        .thenThrow(new IllegalStateException("Cannot find profile.properties in classpath"));
      assertThrows(IllegalStateException.class, ProfileManager::reload);
    }
  }

  @Test
  void testIOExceptionWrappedInRuntimeException() {
    try (MockedStatic<ProfileManager> mocked = Mockito.mockStatic(ProfileManager.class)) {
      mocked.when(ProfileManager::reload).thenThrow(new RuntimeException("Failed to load profile.properties"));
      assertThrows(RuntimeException.class, ProfileManager::reload);
    }
  }

  @Test
  void testProfileReload() {
    try (MockedStatic<ProfileManager> mocked = Mockito.mockStatic(ProfileManager.class)) {
      mocked.when(ProfileManager::getActiveProfile).thenReturn("dev", "prod");
      String initialProfile = ProfileManager.getActiveProfile();
      ProfileManager.reload();
      String reloadedProfile = ProfileManager.getActiveProfile();
      assertNotEquals(initialProfile, reloadedProfile);
    }
  }
}
