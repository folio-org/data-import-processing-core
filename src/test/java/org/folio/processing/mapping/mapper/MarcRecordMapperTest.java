package org.folio.processing.mapping.mapper;

import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class MarcRecordMapperTest {

  private MarcRecordMapper mapper = new MarcRecordMapper();

  @Test
  public void shouldReturnEmptyListIfThereIsNoSettings() {
    List<MarcFieldProtectionSetting> marcFieldProtectionSettings = new ArrayList<>();
    List<MarcFieldProtectionSetting> protectionSettingsOverrides = Collections.singletonList(
      new MarcFieldProtectionSetting()
        .withId(UUID.randomUUID().toString())
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(true));

    assertTrue(mapper.filterOutOverriddenProtectionSettings(marcFieldProtectionSettings, protectionSettingsOverrides).isEmpty());
  }

  @Test
  public void shouldReturnSameSettingsIfNoOverrides() {
    List<MarcFieldProtectionSetting> marcFieldProtectionSettings = Collections.singletonList(
      new MarcFieldProtectionSetting()
        .withId(UUID.randomUUID().toString())
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false));
    List<MarcFieldProtectionSetting> protectionSettingsOverrides = new ArrayList<>();

    assertEquals(marcFieldProtectionSettings, mapper.filterOutOverriddenProtectionSettings(marcFieldProtectionSettings, protectionSettingsOverrides));
  }

  @Test
  public void shouldFilterOutOverriddenFieldProtectionSettings() {
    List<MarcFieldProtectionSetting> marcFieldProtectionSettings = Arrays.asList(
      new MarcFieldProtectionSetting()
        .withId("76669a02-a3d4-41af-9392-58502eaacd10")
        .withField("001")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.SYSTEM)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("480f0b23-0cbe-4a5c-b1f1-568b3216ff68")
        .withField("999")
        .withIndicator1("f")
        .withIndicator2("f")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.SYSTEM)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("2ef38de1-73aa-4e02-ae37-44e7148f414e")
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("c4bd5ddb-55de-467a-b824-c9e58822d006")
        .withField("650")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("6a13e600-a126-4d02-bc16-abd9ea7bed7c")
        .withField("700")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("bdd4b0cb-f598-4d6b-bbbe-3bcfc658e85f")
        .withField("035")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false));

    List<MarcFieldProtectionSetting> protectionSettingsOverrides = Arrays.asList(
      new MarcFieldProtectionSetting()
        .withId("2ef38de1-73aa-4e02-ae37-44e7148f414e")
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(true),
      new MarcFieldProtectionSetting()
        .withId("c4bd5ddb-55de-467a-b824-c9e58822d006")
        .withField("650")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(true),
      new MarcFieldProtectionSetting()
        .withId("480f0b23-0cbe-4a5c-b1f1-568b3216ff68")
        .withField("999")
        .withIndicator1("f")
        .withIndicator2("f")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.SYSTEM)
        .withOverride(true),
      new MarcFieldProtectionSetting()
        .withId("bdd4b0cb-f598-4d6b-bbbe-3bcfc658e85f")
        .withField("035")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("2557b110-df80-496d-aa04-d6549bc13a28")
        .withField("040")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(true)
      );

    List<MarcFieldProtectionSetting> expectedRelevantProtectionSettings = Arrays.asList(
      new MarcFieldProtectionSetting()
        .withId("76669a02-a3d4-41af-9392-58502eaacd10")
        .withField("001")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.SYSTEM)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("480f0b23-0cbe-4a5c-b1f1-568b3216ff68")
        .withField("999")
        .withIndicator1("f")
        .withIndicator2("f")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.SYSTEM)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("6a13e600-a126-4d02-bc16-abd9ea7bed7c")
        .withField("700")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false),
      new MarcFieldProtectionSetting()
        .withId("bdd4b0cb-f598-4d6b-bbbe-3bcfc658e85f")
        .withField("035")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfield("*")
        .withData("*")
        .withSource(MarcFieldProtectionSetting.Source.USER)
        .withOverride(false)
    );

    List<MarcFieldProtectionSetting> actual =
      mapper.filterOutOverriddenProtectionSettings(marcFieldProtectionSettings, protectionSettingsOverrides);

    assertEquals(expectedRelevantProtectionSettings.size(), actual.size());
    expectedRelevantProtectionSettings.forEach(setting ->
      assertTrue(actual.stream().anyMatch(actualSetting -> setting.getId().equals(actualSetting.getId()))));
  }

}
