package org.folio.processing.mapping.mapper.writer.marc;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.Record;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcWriter;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Leader;
import org.marc4j.marc.MarcFactory;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.VariableField;
import org.marc4j.marc.impl.Verifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.processing.value.Value.ValueType.MARC_DETAIL;

public class MarcRecordWriter implements Writer {
  private static final Logger LOGGER = LoggerFactory.getLogger(MarcRecordWriter.class);
  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Can not initialize MarcRecordWriter, cause event payload context does not contain MARC_BIBLIOGRAPHIC data";

  private static final char[] SORTABLE_FIELDS_FIRST_DIGITS = new char[]{'0', '1', '2', '3', '9'};
  private static final char BLANK_SUBFIELD_CODE = ' ';
  private static final String LDR_TAG = "LDR";

  private String entityType;
  private Record sourceRecord;
  private org.marc4j.marc.Record marcRecord;
  private MarcFactory marcFactory = MarcFactory.newInstance();


  public MarcRecordWriter(EntityType entityType) {
    this.entityType = entityType.value();
  }

  @Override
  public void initialize(DataImportEventPayload eventPayload) throws IOException {
    if (isNull(eventPayload.getContext()) || isBlank(eventPayload.getContext().get(entityType))) {
      LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
      throw new IllegalArgumentException(PAYLOAD_HAS_NO_DATA_MSG);
    }

    String recordAsString = eventPayload.getContext().get(entityType);
    sourceRecord = ObjectMapperTool.getMapper().readValue(recordAsString, Record.class);
    if (nonNull(sourceRecord.getParsedRecord()) && isNotBlank(sourceRecord.getParsedRecord().getContent().toString())) {
      MarcReader marcReader = buildMarcReader(sourceRecord);
      if (marcReader.hasNext()) {
        this.marcRecord = marcReader.next();
      }
    }
  }

  private MarcReader buildMarcReader(org.folio.Record record) {
    return new MarcJsonReader(new ByteArrayInputStream(
      record.getParsedRecord()
        .getContent()
        .toString()
        .getBytes(StandardCharsets.UTF_8)));
  }

  @Override
  public void write(String fieldPath, Value value) {
    if (value.getType() != MARC_DETAIL) {
      throw new IllegalArgumentException("Unsupported value type, it should be 'MARC_DETAIL' value type");
    }

    MarcMappingDetail mappingDetail = (MarcMappingDetail) value.getValue();
    switch (mappingDetail.getAction()) {
      case ADD:
        processAddAction(mappingDetail);
        break;
      case DELETE:
        processDeleteAction(mappingDetail);
        break;
      case EDIT:
        processEditAction(mappingDetail);
        break;
      case MOVE:
        processMoveAction(mappingDetail);
    }
  }

  @Override
  public DataImportEventPayload getResult(DataImportEventPayload eventPayload) {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
      MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
      streamWriter.write(marcRecord);
      jsonWriter.write(marcRecord);
      sourceRecord.getParsedRecord().setContent(new JsonObject(new String(os.toByteArray())).encode());
      eventPayload.getContext().put(entityType, Json.encode(sourceRecord));
    } catch (Exception e) {
      LOGGER.error("Can not put the newly mapped record to the event payload", e);
      throw new IllegalStateException(e);
    }
    return eventPayload;
  }

  private void processAddAction(MarcMappingDetail detail) {
    String fieldTag = detail.getField().getField();
    if (Verifier.isControlField(fieldTag)) {
      ControlField controlField = marcFactory.newControlField(fieldTag, detail.getField().getSubfields().get(0).getData().getText());
      addFieldInNumericalOrder(controlField);
    } else {
      char ind1 = isNotEmpty(detail.getField().getIndicator1()) ? detail.getField().getIndicator1().charAt(0) : BLANK_SUBFIELD_CODE;
      char ind2 = isNotEmpty(detail.getField().getIndicator2()) ? detail.getField().getIndicator2().charAt(0) : BLANK_SUBFIELD_CODE;
      DataField dataField = marcFactory.newDataField(fieldTag, ind1, ind2);

      for (MarcSubfield subfield : detail.getField().getSubfields()) {
        dataField.addSubfield(marcFactory.newSubfield(subfield.getSubfield().charAt(0), subfield.getData().getText()));
      }
      addFieldInNumericalOrder(dataField);
    }
  }

  private void addFieldInNumericalOrder(ControlField field) {
    List<ControlField> controlFields = marcRecord.getControlFields();
    for (int i = 0; i < controlFields.size(); i++) {
      if (controlFields.get(i).getTag().compareTo(field.getTag()) > 0) {
        marcRecord.getControlFields().add(i, field);
        return;
      }
    }
    marcRecord.addVariableField(field);
  }

  /**
   * Adds data field to record in numerical order when the specified field is kind of sortable field (0xx, 1xx, 2xx, 3xx, 9xx),
   * which can be placed in numerical order. If the specified field should not be sorted (4xx, 5xx, 6xx, 7xx, 8xx),
   * then the field is added at the end of other fields (7xx) with the same first digit (7).
   * For instance, specified field 500 will be added to existing record fields in such order:
   * 490\\$adata
   * 507\\$adata
   * 500\\$adata
   * 650\\$adata
   *
   * @param field data field for adding to a record
   */
  private void addFieldInNumericalOrder(VariableField field) {
    String tag = field.getTag();
    List<DataField> dataFields = marcRecord.getDataFields();
    if (isNumericalSortableField(field)) {
      for (int i = 0; i < dataFields.size(); i++) {
        if (dataFields.get(i).getTag().compareTo(tag) > 0) {
          marcRecord.getDataFields().add(i, (DataField) field);
          return;
        }
      }
    } else {
      for (int i = 0; i < dataFields.size(); i++) {
        if (dataFields.get(i).getTag().charAt(0) > tag.charAt(0)) {
          marcRecord.getDataFields().add(i, (DataField) field);
          return;
        }
      }
    }
    marcRecord.addVariableField(field);
  }

  private boolean isNumericalSortableField(VariableField field) {
    return ArrayUtils.contains(SORTABLE_FIELDS_FIRST_DIGITS, field.getTag().charAt(0));
  }

  private void processDeleteAction(MarcMappingDetail detail) {
    String fieldTag = detail.getField().getField();
    char ind1 = isNotEmpty(detail.getField().getIndicator1()) ? detail.getField().getIndicator1().charAt(0) : BLANK_SUBFIELD_CODE;
    char ind2 = isNotEmpty(detail.getField().getIndicator2()) ? detail.getField().getIndicator2().charAt(0) : BLANK_SUBFIELD_CODE;

    if (Verifier.isControlField(fieldTag)) {
      for (VariableField field : marcRecord.getVariableFields(fieldTag)) {
        marcRecord.removeVariableField(field);
      }
    } else if (detail.getField().getSubfields().get(0).getSubfield().charAt(0) == '*') {
      marcRecord.getDataFields().stream()
        .filter(field -> fieldMatches(field, fieldTag, ind1, ind2))
        .collect(Collectors.toList())
        .forEach(fieldToDelete -> marcRecord.removeVariableField(fieldToDelete));
    } else {
      char subfieldCode = detail.getField().getSubfields().get(0).getSubfield().charAt(0);
      marcRecord.getDataFields().stream()
        .filter(field -> fieldMatches(field, fieldTag, ind1, ind2))
        .peek(targetField -> targetField.removeSubfield(targetField.getSubfield(subfieldCode)))
        .filter(field -> field.getSubfields().isEmpty())
        .collect(Collectors.toList())
        .forEach(targetField -> marcRecord.removeVariableField(targetField));
    }
  }

  private boolean fieldMatches(DataField field, String tag, char ind1, char ind2) {
    if (!field.getTag().equals(tag)) {
      return false;
    }
    if (ind1 != '*' && field.getIndicator1() != ind1) {
      return false;
    }
    return ind2 == '*' || field.getIndicator2() == ind2;
  }

  private void processEditAction(MarcMappingDetail mappingDetail) {
    MarcSubfield subfieldRule = mappingDetail.getField().getSubfields().get(0);
    switch (subfieldRule.getSubaction()) {
      case INSERT:
        processInsert(subfieldRule, mappingDetail);
        break;
      case REPLACE:
        processReplace(mappingDetail);
        break;
      case REMOVE:
        processRemove(mappingDetail);
      default:
    }
  }

  private void processInsert(MarcSubfield ruleSubfield, MarcMappingDetail mappingRule) {
    String tag = mappingRule.getField().getField();
    char ind1 = isNotEmpty(mappingRule.getField().getIndicator1()) ? mappingRule.getField().getIndicator1().charAt(0) : BLANK_SUBFIELD_CODE;
    char ind2 = isNotEmpty(mappingRule.getField().getIndicator2()) ? mappingRule.getField().getIndicator2().charAt(0) : BLANK_SUBFIELD_CODE;
    String dataToInsert = mappingRule.getField().getSubfields().get(0).getData().getText();
    MarcSubfield.Position dataPosition = mappingRule.getField().getSubfields().get(0).getPosition();

    List<DataField> fieldsToEdit = marcRecord.getDataFields().stream()
      .filter(field -> fieldMatches(field, tag, ind1, ind2))
      .collect(Collectors.toList());

    for (DataField field : fieldsToEdit) {
      Subfield subfieldToEdit = field.getSubfield(ruleSubfield.getSubfield().charAt(0));
      switch (dataPosition) {
        case BEFORE_STRING:
          if (subfieldToEdit != null) {
            subfieldToEdit.setData(dataToInsert + subfieldToEdit.getData());
          }
          break;
        case AFTER_STRING:
          if (subfieldToEdit != null) {
            subfieldToEdit.setData(subfieldToEdit.getData() + dataToInsert);
          }
          break;
        case NEW_SUBFIELD:
          field.addSubfield(marcFactory.newSubfield(ruleSubfield.getSubfield().charAt(0), dataToInsert));
      }
    }
  }

  private void processReplace(MarcMappingDetail mappingRule) {
    String tag = mappingRule.getField().getField().substring(0, 3);
    String dataToReplace = mappingRule.getField().getSubfields().get(0).getData().getFind();
    String replacementData = mappingRule.getField().getSubfields().get(0).getData().getReplaceWith();

    if (LDR_TAG.equals(tag)) {
      Range<Integer> positions = getControlFieldDataPosition(mappingRule.getField().getField());
      if (positions.isOverlappedBy(Range.between(0, 4)) || positions.isOverlappedBy(Range.between(12, 16))) {
        LOGGER.warn("Specified LEADER positions are not mappable LDR/{}-{}, REPLACE sub-action was skipped", positions.getMinimum(), positions.getMaximum());
        return;
      }

      Leader leader = marcRecord.getLeader();
      String leaderAsString = leader.marshal();
      boolean dataToReplaceExists = dataToReplace.equals("*") || leaderAsString.substring(positions.getMinimum(), positions.getMaximum() + 1).equals(dataToReplace);
      if (dataToReplaceExists) {
        StringBuilder newData = new StringBuilder(leaderAsString).replace(positions.getMinimum(), positions.getMaximum() + 1, replacementData);
        leader.unmarshal(newData.toString());
      }
    } else if (Verifier.isControlField(tag)) {
      Range<Integer> positions = getControlFieldDataPosition(mappingRule.getField().getField());
      int startPosition = positions.getMinimum();
      int endPosition = positions.getMaximum();

      marcRecord.getControlFields().stream()
        .filter(field -> field.getTag().equals(tag) && dataToReplace.equals("*") || controlFieldContainsDataAtPositions(field, dataToReplace, positions))
        .forEach(fieldToEdit -> {
          StringBuilder newData = new StringBuilder(fieldToEdit.getData()).replace(startPosition, endPosition + 1, replacementData);
          fieldToEdit.setData(newData.toString());
        });
    } else {
      replaceDataInDataFields(tag, dataToReplace, replacementData, mappingRule);
    }
  }

  private void processRemove(MarcMappingDetail mappingRule) {
    String tag = mappingRule.getField().getField().substring(0, 3);
    String dataToRemove = mappingRule.getField().getSubfields().get(0).getData().getText();

    if (Verifier.isControlField(tag)) {
      Range<Integer> positions = getControlFieldDataPosition(mappingRule.getField().getField());
      marcRecord.getControlFields().stream()
        .filter(field -> field.getTag().equals(tag) && controlFieldContainsDataAtPositions(field, dataToRemove, positions))
        .forEach(fieldToEdit -> fieldToEdit.setData(new StringBuilder(fieldToEdit.getData()).delete(positions.getMinimum(), positions.getMaximum() + 1).toString()));
    } else {
      replaceDataInDataFields(tag, dataToRemove, EMPTY, mappingRule);
    }
  }

  /**
   * Receives path to a control field data in the following formats:
   * 008/21 (008 field, 21 single data position),
   * 008/07-10(008 field, 07-10 multiple positions).
   * Extracts position of data in a field and returns it as positions range.
   *
   * @param fieldDataPath path to field data
   * @return range of data positions
   */
  private Range<Integer> getControlFieldDataPosition(String fieldDataPath) {
    int startPosition;
    int endPosition;
    if (fieldDataPath.contains("-")) {
      startPosition = Integer.parseInt(StringUtils.substringBetween(fieldDataPath, "/", "-"));
      endPosition = Integer.parseInt(StringUtils.substringAfter(fieldDataPath, "-"));
    } else {
      startPosition = Integer.parseInt(StringUtils.substringAfter(fieldDataPath, "/"));
      endPosition = startPosition;
    }
    return Range.between(startPosition, endPosition);
  }

  private boolean controlFieldContainsDataAtPositions(ControlField f, String data, Range<Integer> dataPositions) {
    return f.getData().substring(dataPositions.getMinimum(), dataPositions.getMaximum() + 1).equals(data);
  }

  private void replaceDataInDataFields(String tag, String dataToReplace, String replacementData, MarcMappingDetail mappingRule) {
    char ind1 = isNotEmpty(mappingRule.getField().getIndicator1()) ? mappingRule.getField().getIndicator1().charAt(0) : BLANK_SUBFIELD_CODE;
    char ind2 = isNotEmpty(mappingRule.getField().getIndicator2()) ? mappingRule.getField().getIndicator2().charAt(0) : BLANK_SUBFIELD_CODE;
    char subfieldCode = mappingRule.getField().getSubfields().get(0).getSubfield().charAt(0);

    marcRecord.getDataFields().stream()
      .filter(field -> fieldMatches(field, tag, ind1, ind2, subfieldCode))
      .flatMap(fieldToEdit -> findSubfields(fieldToEdit, subfieldCode, dataToReplace).stream())
      .forEach(sf -> sf.setData(dataToReplace.equals("*") ? replacementData : sf.getData().replace(dataToReplace, replacementData)));
  }

  private boolean fieldMatches(DataField field, String tag, char ind1, char ind2, char subfieldCode) {
    if (!fieldMatches(field, tag, ind1, ind2)) {
      return false;
    }
    return subfieldCode == '*' || field.getSubfield(subfieldCode) != null;
  }

  private List<Subfield> findSubfields(DataField field, char subfieldCode, String subfieldDataFragment) {
    List<Subfield> subfieldsForSearch = subfieldCode == '*' ? field.getSubfields() : field.getSubfields(subfieldCode);
    return subfieldsForSearch.stream()
      .filter(sf -> subfieldDataFragment.charAt(0) == '*' || sf.getData().contains(subfieldDataFragment))
      .collect(Collectors.toList());
  }

  private void processMoveAction(MarcMappingDetail detail) {
    char ind1 = isNotEmpty(detail.getField().getIndicator1()) ? detail.getField().getIndicator1().charAt(0) : BLANK_SUBFIELD_CODE;
    char ind2 = isNotEmpty(detail.getField().getIndicator2()) ? detail.getField().getIndicator2().charAt(0) : BLANK_SUBFIELD_CODE;

    List<DataField> sourceFields = marcRecord.getDataFields().stream()
      .filter(field -> fieldMatches(field, detail.getField().getField(), ind1, ind2))
      .collect(Collectors.toList());

    for (MarcSubfield subfieldRule : detail.getField().getSubfields()) {
      switch (subfieldRule.getSubaction()) {
        case CREATE_NEW_FIELD:
          moveDataToNewField(sourceFields, subfieldRule);
          break;
        case ADD_TO_EXISTING_FIELD:
          moveDataToExistingField(sourceFields, subfieldRule);
          break;
        default:
      }
    }
  }

  private void moveDataToNewField(List<DataField> sourceFields, MarcSubfield subfieldRule) {
    MarcField newFieldRule = subfieldRule.getData().getMarcField();
    String newFieldTag = newFieldRule.getField();
    char srcSubfieldCode = subfieldRule.getSubfield().charAt(0);
    char newSubfieldCode = newFieldRule.getSubfields().isEmpty() ? srcSubfieldCode : newFieldRule.getSubfields().get(0).getSubfield().charAt(0);

    for (DataField sourceField : sourceFields) {
      char newFieldInd1 = isNotEmpty(newFieldRule.getIndicator1()) ? newFieldRule.getIndicator1().charAt(0) : sourceField.getIndicator1();
      char newFieldInd2 = isNotEmpty(newFieldRule.getIndicator2()) ? newFieldRule.getIndicator1().charAt(0) : sourceField.getIndicator2();
      DataField newField = marcFactory.newDataField(newFieldTag, newFieldInd1, newFieldInd2);
      List<Subfield> srcSubfields = srcSubfieldCode == '*' ? sourceField.getSubfields() : sourceField.getSubfields(srcSubfieldCode);

      for (Subfield srcSubfield : srcSubfields) {
        Subfield subfieldToMove = srcSubfieldCode == '*' ? srcSubfield : marcFactory.newSubfield(newSubfieldCode, srcSubfield.getData());
        newField.addSubfield(subfieldToMove);
      }
      if (!srcSubfields.isEmpty()) {
        addFieldInNumericalOrder(newField);
        deleteMovedDataFromSourceField(sourceField, srcSubfields, srcSubfieldCode);
      }
    }
  }

  private void moveDataToExistingField(List<DataField> sourceFields, MarcSubfield subfieldRule) {
    String existingFieldTag = subfieldRule.getData().getMarcField().getField();
    char existingFieldInd1 = isEmpty(subfieldRule.getData().getMarcField().getIndicator1()) ? BLANK_SUBFIELD_CODE
      : subfieldRule.getData().getMarcField().getIndicator1().charAt(0);
    char existingFieldInd2 = isEmpty(subfieldRule.getData().getMarcField().getIndicator2()) ? BLANK_SUBFIELD_CODE
      : subfieldRule.getData().getMarcField().getIndicator2().charAt(0);
    char srcSubfieldCode = subfieldRule.getSubfield().charAt(0);
    char existingFieldSfCode = subfieldRule.getData().getMarcField().getSubfields().get(0).getSubfield().charAt(0);

    List<DataField> existingFields = marcRecord.getDataFields().stream()
      .filter(field -> fieldMatches(field, existingFieldTag, existingFieldInd1, existingFieldInd2))
      .collect(Collectors.toList());

    if (!existingFields.isEmpty()) {
      for (DataField sourceField : sourceFields) {
        List<Subfield> srcSubfields = srcSubfieldCode == '*' ? sourceField.getSubfields() : sourceField.getSubfields(srcSubfieldCode);
        for (Subfield srcSubfield : srcSubfields) {
          for (DataField existingField : existingFields) {
            existingField.addSubfield(marcFactory.newSubfield(existingFieldSfCode, srcSubfield.getData()));
          }
        }
        deleteMovedDataFromSourceField(sourceField, srcSubfields, srcSubfieldCode);
      }
    }
  }

  private void deleteMovedDataFromSourceField(DataField sourceField, List<Subfield> movedDataSubfields, char srcSubfieldCodeFromRule) {
    if (srcSubfieldCodeFromRule == '*' || sourceField.getSubfields().size() == movedDataSubfields.size()) {
      marcRecord.removeVariableField(sourceField);
    } else {
      movedDataSubfields.forEach(sourceField::removeSubfield);
    }
  }

}
