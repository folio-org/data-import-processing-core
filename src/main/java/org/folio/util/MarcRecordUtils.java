package org.folio.util;

import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.VariableField;
import org.marc4j.marc.impl.RecordImpl;
import org.marc4j.marc.impl.Verifier;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MarcRecordUtils {

  private static final Logger LOGGER = LogManager.getLogger(MarcRecordUtils.class);

  public static void updateRecord(Record srcRecord, Record recordToUpdate, List<MarcFieldProtectionSetting> protectionSettings) {
    org.marc4j.marc.Record srcMarcRecord = mapParsedContentToObjectRepresentation(srcRecord);
    org.marc4j.marc.Record marcRecordToUpdate = mapParsedContentToObjectRepresentation(recordToUpdate);
    RecordImpl resultRecord = new RecordImpl();
    ArrayList<ControlField> resultControlFields = new ArrayList<>();
    ArrayList<DataField> resultDataFields = new ArrayList<>();

    resultRecord.setLeader(marcRecordToUpdate.getLeader());
    for (VariableField field : marcRecordToUpdate.getVariableFields()) {
      Verifier.isControlField(field.getTag()) {
//        if (isFieldProtected(field)) {
//          resultRecord.addVariableField(field);
//        }
      }
    }
  }

  private static org.marc4j.marc.Record mapParsedContentToObjectRepresentation(Record record) {
    MarcReader existingRecordReader = buildMarcReader(record);
    if (existingRecordReader.hasNext()) {
      return existingRecordReader.next();
    } else {
      LOGGER.error("ERROR_RECORD_PARSING_MSG");
      throw new IllegalArgumentException("ERROR_RECORD_PARSING_MSG");
    }
  }

  private static MarcReader buildMarcReader(org.folio.Record record) {
    JsonObject parsedContent = record.getParsedRecord().getContent() instanceof String
      ? new JsonObject(record.getParsedRecord().getContent().toString())
      : JsonObject.mapFrom(record.getParsedRecord().getContent());

    return new MarcJsonReader(new ByteArrayInputStream(parsedContent
      .toString()
      .getBytes(StandardCharsets.UTF_8)));
  }
}
