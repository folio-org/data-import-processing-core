package org.folio.processing.mapping.mapper;

import org.apache.commons.collections4.CollectionUtils;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.value.MarcDetailValue;
import org.folio.rest.jaxrs.model.MarcMappingDetail;

import java.io.IOException;
import java.util.List;

public class MarcRecordMapper implements Mapper {

  @Override
  public DataImportEventPayload map(Reader reader, Writer writer, MappingProfile profile, DataImportEventPayload eventPayload) throws IOException {
    writer.initialize(eventPayload);
    if (profile.getMappingDetails() == null
      || CollectionUtils.isEmpty(profile.getMappingDetails().getMarcMappingDetails())) {
      return eventPayload;
    }
    List<MarcMappingDetail> mappingRules = profile.getMappingDetails().getMarcMappingDetails();
    for (MarcMappingDetail rule : mappingRules) {
      writer.write(rule.getField().getField(), MarcDetailValue.of(rule));
    }
    return writer.getResult(eventPayload);
  }
}
