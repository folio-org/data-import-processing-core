package org.folio.processing.mapping.mapper;

import io.vertx.core.json.JsonObject;
import org.apache.commons.collections4.CollectionUtils;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordWriter;
import org.folio.processing.value.MarcDetailValue;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.MarcMappingDetail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MarcRecordMapper implements Mapper {

  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";

  @Override
  public DataImportEventPayload map(Reader reader, Writer writer, MappingProfile profile, DataImportEventPayload eventPayload) throws IOException {
    if (profile.getMappingDetails() == null
      || CollectionUtils.isEmpty(profile.getMappingDetails().getMarcMappingDetails())) {
      return eventPayload;
    }
    List<MarcFieldProtectionSetting> marcFieldProtectionSettings = new ArrayList<>();
    if (eventPayload.getContext() != null && eventPayload.getContext().get(MAPPING_PARAMS_KEY) != null) {
      MappingParameters mappingParameters = new JsonObject(eventPayload.getContext().get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);
      marcFieldProtectionSettings = mappingParameters.getMarcFieldProtectionSettings();
    }
    List<MarcFieldProtectionSetting> protectionOverrides = profile.getMarcFieldProtectionSettings();
    List<MarcFieldProtectionSetting> relevantProtectionSettings = filterOutOverriddenProtectionSettings(marcFieldProtectionSettings,
      protectionOverrides);
    MarcRecordWriter marcRecordWriter = (MarcRecordWriter) writer;
    marcRecordWriter.initializeWithProtectionSettings(eventPayload, relevantProtectionSettings);
    List<MarcMappingDetail> mappingRules = profile.getMappingDetails().getMarcMappingDetails();
    for (MarcMappingDetail rule : mappingRules) {
      marcRecordWriter.write(rule.getField().getField(), MarcDetailValue.of(rule));
    }
    return marcRecordWriter.getResult(eventPayload);
  }

  protected List<MarcFieldProtectionSetting> filterOutOverriddenProtectionSettings(List<MarcFieldProtectionSetting> marcFieldProtectionSettings,
                                                                                 List<MarcFieldProtectionSetting> protectionOverrides) {
    return marcFieldProtectionSettings.stream()
      .filter(originalSetting -> protectionOverrides.stream()
        .noneMatch(overriddenSetting -> overriddenSetting.getId().equals(originalSetting.getId())
          && overriddenSetting.getSource().equals(MarcFieldProtectionSetting.Source.USER)
          && overriddenSetting.getOverride()))
      .collect(Collectors.toList());
  }
}
