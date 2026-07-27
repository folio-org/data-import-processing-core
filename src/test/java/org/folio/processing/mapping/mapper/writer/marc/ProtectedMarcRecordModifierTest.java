package org.folio.processing.mapping.mapper.writer.marc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting.Source;
import org.junit.jupiter.api.Test;

class ProtectedMarcRecordModifierTest {

  private final MarcRecordModifier marcRecordModifier = new MarcRecordModifier();

  @Test
  void shouldReturnEmptyListIfThereIsNoSettings() {
    List<MarcFieldProtectionSetting> marcFieldProtectionSettings = new ArrayList<>();
    List<MarcFieldProtectionSetting> protectionSettingsOverrides = Collections.singletonList(
      protectionSetting(UUID.randomUUID().toString(), "020", "*", "*", "*", "*", Source.USER, true));

    assertTrue(
      marcRecordModifier.filterOutOverriddenProtectionSettings(marcFieldProtectionSettings, protectionSettingsOverrides)
        .isEmpty());
  }

  @Test
  void shouldReturnSameSettingsIfNoOverrides() {
    List<MarcFieldProtectionSetting> marcFieldProtectionSettings = Collections.singletonList(
      protectionSetting(UUID.randomUUID().toString(), "020", "*", "*", "*", "*", Source.USER, false));
    List<MarcFieldProtectionSetting> protectionSettingsOverrides = new ArrayList<>();

    assertEquals(marcFieldProtectionSettings,
      marcRecordModifier.filterOutOverriddenProtectionSettings(marcFieldProtectionSettings,
        protectionSettingsOverrides));
  }

  @Test
  void shouldFilterOutOverriddenFieldProtectionSettings() {
    List<MarcFieldProtectionSetting> marcFieldProtectionSettings = getMarcFieldProtectionSettings();

    List<MarcFieldProtectionSetting> protectionSettingsOverrides = getProtectionSettingsOverrides();

    List<MarcFieldProtectionSetting> expectedRelevantProtectionSettings = getExpectedRelevantProtectionSettings();

    List<MarcFieldProtectionSetting> actual =
      marcRecordModifier.filterOutOverriddenProtectionSettings(marcFieldProtectionSettings,
        protectionSettingsOverrides);

    assertEquals(expectedRelevantProtectionSettings.size(), actual.size());
    expectedRelevantProtectionSettings.forEach(setting ->
      assertTrue(actual.stream().anyMatch(actualSetting -> setting.getId().equals(actualSetting.getId()))));
  }

  private List<MarcFieldProtectionSetting> getMarcFieldProtectionSettings() {
    return List.of(
      protectionSetting("76669a02-a3d4-41af-9392-58502eaacd10", "001", null, null, null, "*", Source.SYSTEM, false),
      protectionSetting("480f0b23-0cbe-4a5c-b1f1-568b3216ff68", "999", "f", "f", "*", "*", Source.SYSTEM, false),
      protectionSetting("2ef38de1-73aa-4e02-ae37-44e7148f414e", "020", "*", "*", "*", "*", Source.USER, false),
      protectionSetting("c4bd5ddb-55de-467a-b824-c9e58822d006", "650", "*", "*", "*", "*", Source.USER, false),
      protectionSetting("6a13e600-a126-4d02-bc16-abd9ea7bed7c", "700", "*", "*", "*", "*", Source.USER, false),
      protectionSetting("bdd4b0cb-f598-4d6b-bbbe-3bcfc658e85f", "035", "*", "*", "*", "*", Source.USER, false));
  }

  private List<MarcFieldProtectionSetting> getProtectionSettingsOverrides() {
    return List.of(
      protectionSetting("2ef38de1-73aa-4e02-ae37-44e7148f414e", "020", "*", "*", "*", "*", Source.USER, true),
      protectionSetting("c4bd5ddb-55de-467a-b824-c9e58822d006", "650", "*", "*", "*", "*", Source.USER, true),
      protectionSetting("480f0b23-0cbe-4a5c-b1f1-568b3216ff68", "999", "f", "f", "*", "*", Source.SYSTEM, true),
      protectionSetting("bdd4b0cb-f598-4d6b-bbbe-3bcfc658e85f", "035", "*", "*", "*", "*", Source.USER, false),
      protectionSetting("2557b110-df80-496d-aa04-d6549bc13a28", "040", "*", "*", "*", "*", Source.USER, true));
  }

  private List<MarcFieldProtectionSetting> getExpectedRelevantProtectionSettings() {
    return List.of(
      protectionSetting("76669a02-a3d4-41af-9392-58502eaacd10", "001", null, null, null, "*", Source.SYSTEM, false),
      protectionSetting("480f0b23-0cbe-4a5c-b1f1-568b3216ff68", "999", "f", "f", "*", "*", Source.SYSTEM, false),
      protectionSetting("6a13e600-a126-4d02-bc16-abd9ea7bed7c", "700", "*", "*", "*", "*", Source.USER, false),
      protectionSetting("bdd4b0cb-f598-4d6b-bbbe-3bcfc658e85f", "035", "*", "*", "*", "*", Source.USER, false));
  }

  private MarcFieldProtectionSetting protectionSetting(
    String id,
    String field,
    String indicator1,
    String indicator2,
    String subfield,
    String data,
    Source source,
    boolean override
  ) {
    MarcFieldProtectionSetting setting = new MarcFieldProtectionSetting()
      .withId(id)
      .withField(field)
      .withData(data)
      .withSource(source)
      .withOverride(override);
    if (indicator1 != null) {
      setting.withIndicator1(indicator1);
    }
    if (indicator2 != null) {
      setting.withIndicator2(indicator2);
    }
    if (subfield != null) {
      setting.withSubfield(subfield);
    }
    return setting;
  }
}
