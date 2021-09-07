package org.folio.processing.mapping.generateHoldings;

import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.Record;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcHoldingsReaderFactory;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.processing.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.MARC_HOLDINGS;

public class GenerateHoldingsTest {

  private static final String MAPPING_PROFILE_PATH = "src/test/resources/org/folio/processing/mapping/holdings/mappingProfile.json";
  private static final String SRS_HOLDINGS_RECORD_PATH = "src/test/resources/org/folio/processing/mapping/holdings/srsHoldingsRecord.json";
  private static final String GENERATED_HOLDINGS_RECORD_PATH = "src/test/resources/org/folio/processing/mapping/holdings/generatedHoldingsRecord.json";
  private MappingProfile mappingProfile;
  private Record srsHoldingsRecord;
  private JsonObject generatedHoldingsRecord;

  @Before
  public void init() throws IOException {
    this.mappingProfile = new JsonObject(TestUtil.readFileFromPath(MAPPING_PROFILE_PATH)).mapTo(MappingProfile.class);
    this.srsHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(SRS_HOLDINGS_RECORD_PATH)).mapTo(Record.class);
    this.generatedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(GENERATED_HOLDINGS_RECORD_PATH));
  }

  @Test
  public void test() {
    // given
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), JsonObject.mapFrom(srsHoldingsRecord).encode());
    context.put(HOLDINGS.value(), "{}");
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper();
    profileSnapshotWrapper.setContentType(ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE);
    profileSnapshotWrapper.setContent(JsonObject.mapFrom(mappingProfile).getMap());
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper);

    // when
    MappingManager.registerReaderFactory(new MarcHoldingsReaderFactory());
    MappingManager.registerWriterFactory(new HoldingsWriterFactory());
    MappingManager.map(dataImportEventPayload);
    JsonObject mappedRecord = new JsonObject(dataImportEventPayload.getContext().get(EntityType.HOLDINGS.value()));

    //then
    Assert.assertEquals(generatedHoldingsRecord, mappedRecord);
  }

}
