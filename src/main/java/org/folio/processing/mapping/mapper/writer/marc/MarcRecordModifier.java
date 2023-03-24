package org.folio.processing.mapping.mapper.writer.marc;

import static java.lang.Character.isDigit;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import static org.folio.rest.jaxrs.model.MappingDetail.MarcMappingOption.MODIFY;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.marc4j.marc.impl.SubfieldImpl;
import org.marc4j.marc.impl.Verifier;

import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.Record;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;

public class MarcRecordModifier {

  private static final Logger LOGGER = LogManager.getLogger(MarcRecordModifier.class);

  private static final String ERROR_RECORD_PARSING_MSG = "Failed to parse record from payload";
  private static final String PAYLOAD_HAS_NO_DATA_MSG =
    "Cannot initialize MarcRecordModifier - event payload context does not contain required data";
  private static final char[] SORTABLE_FIELDS_FIRST_DIGITS = new char[] {'0', '1', '2', '3', '9'};
  private static final Set<String> NON_REPEATABLE_CONTROL_FIELDS_TAGS =
    Set.of("001", "002", "003", "004", "005", "008", "009");
  public static final Set<String> NON_REPEATABLE_DATA_FIELDS_TAGS = Set.of("010", "018", "036", "038", "040", "042",
    "044", "045", "066", "073", "100", "110", "111", "130", "240", "243", "245", "254", "256", "263", "306", "357",
    "378", "384", "507", "514", "663", "664", "665", "666", "675", "682", "788", "841", "842", "844", "882", "999");
  protected static final String ANY_STRING = "*";
  private static final char BLANK_SUBFIELD_CODE = ' ';
  private static final String LDR_TAG = "LDR";
  private static final String TAG_100 = "100";
  private static final String TAG_199 = "199";
  private static final String TAG_999 = "999";
  private static final char INDICATOR_F = 'f';
  protected static final char ANY_CHAR = '*';

  private final MarcFactory marcFactory = MarcFactory.newInstance();

  private MappingDetail.MarcMappingOption marcMappingOption;
  private Record recordToChange;
  private org.marc4j.marc.Record incomingMarcRecord;
  private org.marc4j.marc.Record marcRecordToChange;
  private List<MarcFieldProtectionSetting> applicableProtectionSettings = new ArrayList<>();
  private DataField fieldToRemove = null;
  private List<DataField> updatedFields = new ArrayList<>();
  private EntityType marcType;

  public void initialize(DataImportEventPayload eventPayload, MappingParameters mappingParameters,
                         MappingProfile mappingProfile, EntityType marcType) throws IOException {
    this.marcMappingOption = mappingProfile.getMappingDetails().getMarcMappingOption();
    this.marcType = marcType;
    switch (mappingProfile.getMappingDetails().getMarcMappingOption()) {
      case MODIFY:
        initializeForModifyOption(eventPayload, mappingParameters, mappingProfile);
        break;
      case UPDATE:
        initializeForUpdateOption(eventPayload, mappingParameters, mappingProfile);
    }
  }

  public void modifyRecord(List<MarcMappingDetail> mappingDetails) {
    switch (marcMappingOption) {
      case MODIFY:
        processModifyMappingOption(mappingDetails);
        break;
      case UPDATE:
        processUpdateMappingOption(mappingDetails);
    }
  }

  public DataImportEventPayload getResult(DataImportEventPayload eventPayload) {
    this.recordToChange.getParsedRecord().setContent(mapRecordRepresentationToJsonString(marcRecordToChange));
    String resultKey = marcMappingOption == MODIFY ? marcType.value() : getMatchedMarcKey();
    eventPayload.getContext().put(resultKey, Json.encode(recordToChange));
    return eventPayload;
  }

  public void processUpdateMappingOption(List<MarcMappingDetail> marcMappingRules) {
    if (marcMappingRules.isEmpty()) {
      replaceAllFields(incomingMarcRecord.getVariableFields());
      return;
    }

    for (MarcMappingDetail detail : marcMappingRules) {
      String fieldTag = detail.getField().getField();
      if (Verifier.isControlField(fieldTag)) {
        incomingMarcRecord.getControlFields().stream()
          .filter(field -> field.getTag().equals(fieldTag))
          .findFirst()
          .ifPresent(this::replaceControlField);
      } else {
        char ind1 =
          isNotEmpty(detail.getField().getIndicator1()) ? detail.getField().getIndicator1().charAt(0) : BLANK_SUBFIELD_CODE;
        char ind2 =
          isNotEmpty(detail.getField().getIndicator2()) ? detail.getField().getIndicator2().charAt(0) : BLANK_SUBFIELD_CODE;
        String subfieldCode = detail.getField().getSubfields().get(0).getSubfield();

        incomingMarcRecord.getDataFields().stream()
          .filter(field -> fieldMatches(field, fieldTag, ind1, ind2, subfieldCode.charAt(0)))
          .findFirst()
          .ifPresent(
            field -> replaceDataField(field, field.getTag(), field.getIndicator1(), field.getIndicator2(), subfieldCode));
      }
    }
  }

  /**
   * Performs update of {@code recordToUpdate} record content with data from {@code srcRecord},
   * applying {@code protectionSettings} to the {@code recordToUpdate} record.
   * It does not change the {@code recordToUpdate} content itself, but returns the updated record content
   * as a new {@code String}.
   * Logic of record update recognizes MARC record fields as repeatable and non-repeatable.
   * Non-repeatable fields: 001, 002, 003, 004, 005, 008, 009, 010, 018, 036, 038, 040, 042, 044, 045, 066, 073,
   * all 1xx fields, 240, 243, 245, 254, 256, 263, 306, 357, 378, 384, 507, 514, 663, 664, 665, 666, 675, 682, 788,
   * 841, 842, 844, 882, and 999 with indicators = ff. Repeatable fields: all other MARC fields.
   *
   * Record update logic is described by following conditions:
   * if field of {@code recordToUpdate} is not protected and there is incoming field with same tag, then delete
   * field from target record and add incoming field with from the {@code srcRecord};
   * if field of {@code recordToUpdate} is not protected and there are no fields with same tag in {@code srcRecord},
   * then delete existing field from target record;
   * if field of {@code recordToUpdate} is protected and there are no fields with same tag in {@code srcRecord},
   * retain existing field;
   * if field of {@code recordToUpdate} is protected and non-repeatable, retain existing field and discard
   * incoming field with same tag from {@code srcRecord};
   * if existing field of {@code recordToUpdate} is protected and repeatable, and incoming field is exactly same
   * as the existing field, then retain existing field and discard incoming field with same tag from {@code srcRecord};
   * if existing field of {@code recordToUpdate} is protected and repeatable, and incoming field is not exactly same
   * as the existing field, then retain existing field and add incoming field with same tag from {@code srcRecord};
   *
   * @param srcRecord          source of data for updating {@code recordToUpdate} content
   * @param recordToUpdate     record whose content should be updated
   * @param protectionSettings marc fields protection settings
   * @return updated content of the {@code recordToUpdate} record as a new {@code String}
   */
  public String updateRecord(Record srcRecord, Record recordToUpdate, List<MarcFieldProtectionSetting> protectionSettings) {
    incomingMarcRecord = readParsedContentToObjectRepresentation(srcRecord);
    marcRecordToChange = readParsedContentToObjectRepresentation(recordToUpdate);
    applicableProtectionSettings = protectionSettings;

    replaceAllFields(incomingMarcRecord.getVariableFields());
    return mapRecordRepresentationToJsonString(marcRecordToChange);
  }

  private String mapRecordRepresentationToJsonString(org.marc4j.marc.Record marcRecord) {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
    MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
    streamWriter.write(marcRecord);
    jsonWriter.write(marcRecord);
    return os.toString().trim();
  }

  protected List<MarcFieldProtectionSetting> filterOutOverriddenProtectionSettings
    (List<MarcFieldProtectionSetting> marcFieldProtectionSettings,
     List<MarcFieldProtectionSetting> protectionOverrides) {
    return marcFieldProtectionSettings.stream()
      .filter(originalSetting -> protectionOverrides.stream()
        .noneMatch(overriddenSetting -> overriddenSetting.getId().equals(originalSetting.getId())
          && overriddenSetting.getSource().equals(MarcFieldProtectionSetting.Source.USER)
          && overriddenSetting.getOverride()))
      .collect(Collectors.toList());
  }

  private String getMatchedMarcKey() {
    return "MATCHED_" + marcType.value();
  }

  private void initializeForModifyOption(DataImportEventPayload eventPayload, MappingParameters mappingParameters,
                                         MappingProfile mappingProfile) throws IOException {
    if (isNull(eventPayload.getContext()) || isBlank(eventPayload.getContext().get(marcType.value()))) {
      LOGGER.warn(PAYLOAD_HAS_NO_DATA_MSG);
      throw new IllegalArgumentException(PAYLOAD_HAS_NO_DATA_MSG);
    }

    String recordAsString = eventPayload.getContext().get(marcType.value());
    recordToChange = new ObjectMapper().readValue(recordAsString, Record.class);
    if (isRecordValid(recordToChange)) {
      this.marcRecordToChange = readParsedContentToObjectRepresentation(recordToChange);
      initMappingParams(mappingParameters, mappingProfile);
    }
  }

  private void initializeForUpdateOption(DataImportEventPayload eventPayload, MappingParameters mappingParameters,
                                         MappingProfile mappingProfile) throws IOException {
    if (isNull(eventPayload.getContext())
      || isBlank(eventPayload.getContext().get(marcType.value()))
      || isBlank(eventPayload.getContext().get(getMatchedMarcKey()))) {
      LOGGER.warn(PAYLOAD_HAS_NO_DATA_MSG);
      throw new IllegalArgumentException(PAYLOAD_HAS_NO_DATA_MSG);
    }

    String incomingRecordAsString = eventPayload.getContext().get(marcType.value());
    String existingRecordAsString = eventPayload.getContext().get(getMatchedMarcKey());
    ObjectMapper objectMapper = new ObjectMapper();
    Record incomingRecord = objectMapper.readValue(incomingRecordAsString, Record.class);
    recordToChange = objectMapper.readValue(existingRecordAsString, Record.class);

    if (isRecordValid(incomingRecord) && isRecordValid(recordToChange)) {
      this.incomingMarcRecord = readParsedContentToObjectRepresentation(incomingRecord);
      this.marcRecordToChange = readParsedContentToObjectRepresentation(recordToChange);
      initMappingParams(mappingParameters, mappingProfile);
    }
  }

  private void initMappingParams(MappingParameters mappingParameters, MappingProfile mappingProfile) {
    List<MarcFieldProtectionSetting> fieldProtectionSettings = mappingParameters.getMarcFieldProtectionSettings();
    List<MarcFieldProtectionSetting> overriddenProtectionSettings = mappingProfile.getMarcFieldProtectionSettings();
    applicableProtectionSettings =
      filterOutOverriddenProtectionSettings(fieldProtectionSettings, overriddenProtectionSettings);
  }

  private boolean isRecordValid(Record marcRecord) {
    return nonNull(marcRecord.getParsedRecord()) && isNotBlank(marcRecord.getParsedRecord().getContent().toString());
  }

  private org.marc4j.marc.Record readParsedContentToObjectRepresentation(Record marcRecord) {
    MarcReader existingRecordReader = buildMarcReader(marcRecord);
    if (existingRecordReader.hasNext()) {
      return existingRecordReader.next();
    } else {
      LOGGER.warn(ERROR_RECORD_PARSING_MSG);
      throw new IllegalArgumentException(ERROR_RECORD_PARSING_MSG);
    }
  }

  private MarcReader buildMarcReader(org.folio.Record marcRecord) {
    JsonObject parsedContent = marcRecord.getParsedRecord().getContent() instanceof String
      ? new JsonObject(marcRecord.getParsedRecord().getContent().toString())
      : JsonObject.mapFrom(marcRecord.getParsedRecord().getContent());

    return new MarcJsonReader(new ByteArrayInputStream(parsedContent
      .toString()
      .getBytes(StandardCharsets.UTF_8)));
  }

  private void processModifyMappingOption(List<MarcMappingDetail> mappingDetails) {
    for (MarcMappingDetail mappingDetail : mappingDetails) {
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
  }

  private void processAddAction(MarcMappingDetail detail) {
    String fieldTag = detail.getField().getField();
    if (Verifier.isControlField(fieldTag)) {
      ControlField controlField =
        marcFactory.newControlField(fieldTag, detail.getField().getSubfields().get(0).getData().getText());
      addControlFieldInNumericalOrder(controlField);
    } else {
      char ind1 =
        isNotEmpty(detail.getField().getIndicator1()) ? detail.getField().getIndicator1().charAt(0) : BLANK_SUBFIELD_CODE;
      char ind2 =
        isNotEmpty(detail.getField().getIndicator2()) ? detail.getField().getIndicator2().charAt(0) : BLANK_SUBFIELD_CODE;
      DataField dataField = marcFactory.newDataField(fieldTag, ind1, ind2);

      for (MarcSubfield subfield : detail.getField().getSubfields()) {
        dataField.addSubfield(marcFactory.newSubfield(subfield.getSubfield().charAt(0), subfield.getData().getText()));
      }
      addDataFieldInNumericalOrder(dataField);
    }
  }

  private void addControlFieldInNumericalOrder(ControlField field) {
    List<ControlField> controlFields = marcRecordToChange.getControlFields();
    for (int i = 0; i < controlFields.size(); i++) {
      if (controlFields.get(i).getTag().compareTo(field.getTag()) > 0) {
        marcRecordToChange.getControlFields().add(i, field);
        return;
      }
    }
    marcRecordToChange.addVariableField(field);
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
  private void addDataFieldInNumericalOrder(DataField field) {
    String tag = field.getTag();
    List<DataField> dataFields = marcRecordToChange.getDataFields();
    if (isNumericalSortableField(field)) {
      for (int i = 0; i < dataFields.size(); i++) {
        if (dataFields.get(i).getTag().compareTo(tag) > 0) {
          marcRecordToChange.getDataFields().add(i, field);
          return;
        }
      }
    } else {
      for (int i = 0; i < dataFields.size(); i++) {
        if (dataFields.get(i).getTag().charAt(0) > tag.charAt(0)) {
          marcRecordToChange.getDataFields().add(i, field);
          return;
        }
      }
    }
    marcRecordToChange.addVariableField(field);
  }

  private boolean isNumericalSortableField(VariableField field) {
    return ArrayUtils.contains(SORTABLE_FIELDS_FIRST_DIGITS, field.getTag().charAt(0));
  }

  private void processDeleteAction(MarcMappingDetail detail) {
    String fieldTag = detail.getField().getField();
    char ind1 =
      isNotEmpty(detail.getField().getIndicator1()) ? detail.getField().getIndicator1().charAt(0) : BLANK_SUBFIELD_CODE;
    char ind2 =
      isNotEmpty(detail.getField().getIndicator2()) ? detail.getField().getIndicator2().charAt(0) : BLANK_SUBFIELD_CODE;

    if (Verifier.isControlField(fieldTag)) {
      for (VariableField field : marcRecordToChange.getVariableFields(fieldTag)) {
        marcRecordToChange.removeVariableField(field);
      }
    } else if (detail.getField().getSubfields().get(0).getSubfield().charAt(0) == ANY_CHAR) {
      marcRecordToChange.getDataFields().stream()
        .filter(field -> fieldMatches(field, fieldTag, ind1, ind2))
        .collect(Collectors.toList())
        .forEach(fieldToDelete -> marcRecordToChange.removeVariableField(fieldToDelete));
    } else {
      char subfieldCode = detail.getField().getSubfields().get(0).getSubfield().charAt(0);
      marcRecordToChange.getDataFields().stream()
        .filter(field -> fieldMatches(field, fieldTag, ind1, ind2))
        .map(targetField -> {
          targetField.removeSubfield(targetField.getSubfield(subfieldCode));
          return targetField;
        })
        .filter(field -> field.getSubfields().isEmpty())
        .collect(Collectors.toList())
        .forEach(targetField -> marcRecordToChange.removeVariableField(targetField));
    }
  }

  private boolean fieldMatches(DataField field, String tag, char ind1, char ind2) {
    if (!field.getTag().equals(tag)) {
      return false;
    }
    if (ind1 != ANY_CHAR && field.getIndicator1() != ind1) {
      return false;
    }
    return ind2 == ANY_CHAR || field.getIndicator2() == ind2;
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
        break;
      default:
    }
  }

  private void processInsert(MarcSubfield ruleSubfield, MarcMappingDetail mappingRule) {
    String tag = mappingRule.getField().getField();
    char ind1 = isNotEmpty(mappingRule.getField().getIndicator1())
      ? mappingRule.getField().getIndicator1().charAt(0)
      : BLANK_SUBFIELD_CODE;
    char ind2 = isNotEmpty(mappingRule.getField().getIndicator2())
      ? mappingRule.getField().getIndicator2().charAt(0)
      : BLANK_SUBFIELD_CODE;
    String dataToInsert = mappingRule.getField().getSubfields().get(0).getData().getText();
    MarcSubfield.Position dataPosition = mappingRule.getField().getSubfields().get(0).getPosition();

    List<DataField> fieldsToEdit = marcRecordToChange.getDataFields().stream()
      .filter(field -> fieldMatches(field, tag, ind1, ind2))
      .collect(Collectors.toList());

    char subfieldCode = ruleSubfield.getSubfield().charAt(0);
    for (DataField field : fieldsToEdit) {
      List<Subfield> subfieldsToEdit = subfieldCode == ANY_CHAR ? field.getSubfields() : field.getSubfields(subfieldCode);
      switch (dataPosition) {
        case BEFORE_STRING:
          subfieldsToEdit.forEach(subfield -> subfield.setData(dataToInsert + subfield.getData()));
          break;
        case AFTER_STRING:
          subfieldsToEdit.forEach(subfield -> subfield.setData(subfield.getData() + dataToInsert));
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
        LOGGER.warn("processReplace:: Specified LEADER positions are not mappable LDR/{}-{}, REPLACE sub-action was skipped",
          positions.getMinimum(), positions.getMaximum());
        return;
      }

      Leader leader = marcRecordToChange.getLeader();
      String leaderAsString = leader.marshal();
      boolean dataToReplaceExists = dataToReplace.equals(ANY_STRING)
        || leaderAsString.substring(positions.getMinimum(), positions.getMaximum() + 1).equals(dataToReplace);
      if (dataToReplaceExists) {
        StringBuilder newData =
          new StringBuilder(leaderAsString).replace(positions.getMinimum(), positions.getMaximum() + 1, replacementData);
        leader.unmarshal(newData.toString());
      }
    } else if (Verifier.isControlField(tag)) {
      Range<Integer> positions = getControlFieldDataPosition(mappingRule.getField().getField());
      int startPosition = positions.getMinimum();
      int endPosition = positions.getMaximum();

      marcRecordToChange.getControlFields().stream()
        .filter(field -> field.getTag().equals(tag) && dataToReplace.equals(ANY_STRING) ||
          controlFieldContainsDataAtPositions(field, dataToReplace, positions))
        .forEach(fieldToEdit -> {
          StringBuilder newData =
            new StringBuilder(fieldToEdit.getData()).replace(startPosition, endPosition + 1, replacementData);
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
      marcRecordToChange.getControlFields().stream()
        .filter(field -> field.getTag().equals(tag) && controlFieldContainsDataAtPositions(field, dataToRemove, positions))
        .forEach(fieldToEdit -> fieldToEdit.setData(
          new StringBuilder(fieldToEdit.getData()).delete(positions.getMinimum(), positions.getMaximum() + 1).toString()));
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

  private boolean controlFieldContainsDataAtPositions(ControlField field, String data, Range<Integer> dataPositions) {
    return field.getData().substring(dataPositions.getMinimum(), dataPositions.getMaximum() + 1).equals(data);
  }

  private void replaceDataInDataFields(String tag, String dataToReplace, String replacementData,
                                       MarcMappingDetail mappingRule) {
    char ind1 = isNotEmpty(mappingRule.getField().getIndicator1())
      ? mappingRule.getField().getIndicator1().charAt(0)
      : BLANK_SUBFIELD_CODE;
    char ind2 = isNotEmpty(mappingRule.getField().getIndicator2())
      ? mappingRule.getField().getIndicator2().charAt(0)
      : BLANK_SUBFIELD_CODE;
    char subfieldCode = mappingRule.getField().getSubfields().get(0).getSubfield().charAt(0);

    marcRecordToChange.getDataFields().stream()
      .filter(field -> fieldMatches(field, tag, ind1, ind2, subfieldCode))
      .flatMap(fieldToEdit -> findSubfields(fieldToEdit, subfieldCode, dataToReplace).stream())
      .forEach(sf -> sf.setData(
        dataToReplace.equals(ANY_STRING) ? replacementData : sf.getData().replace(dataToReplace, replacementData)));
  }

  private boolean fieldMatches(DataField field, String tag, char ind1, char ind2, char subfieldCode) {
    if (!fieldMatches(field, tag, ind1, ind2)) {
      return false;
    }
    return subfieldCode == ANY_CHAR || field.getSubfield(subfieldCode) != null;
  }

  private List<Subfield> findSubfields(DataField field, char subfieldCode, String subfieldDataFragment) {
    List<Subfield> subfieldsForSearch = subfieldCode == ANY_CHAR ? field.getSubfields() : field.getSubfields(subfieldCode);
    return subfieldsForSearch.stream()
      .filter(sf -> subfieldDataFragment.charAt(0) == ANY_CHAR || sf.getData().contains(subfieldDataFragment))
      .collect(Collectors.toList());
  }

  private void processMoveAction(MarcMappingDetail detail) {
    char ind1 =
      isNotEmpty(detail.getField().getIndicator1()) ? detail.getField().getIndicator1().charAt(0) : BLANK_SUBFIELD_CODE;
    char ind2 =
      isNotEmpty(detail.getField().getIndicator2()) ? detail.getField().getIndicator2().charAt(0) : BLANK_SUBFIELD_CODE;

    List<DataField> sourceFields = marcRecordToChange.getDataFields().stream()
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
    char newSubfieldCode =
      newFieldRule.getSubfields().isEmpty() ? srcSubfieldCode : newFieldRule.getSubfields().get(0).getSubfield().charAt(0);

    for (DataField sourceField : sourceFields) {
      char newFieldInd1 =
        isNotEmpty(newFieldRule.getIndicator1()) ? newFieldRule.getIndicator1().charAt(0) : sourceField.getIndicator1();
      char newFieldInd2 =
        isNotEmpty(newFieldRule.getIndicator2()) ? newFieldRule.getIndicator1().charAt(0) : sourceField.getIndicator2();
      DataField newField = marcFactory.newDataField(newFieldTag, newFieldInd1, newFieldInd2);
      List<Subfield> srcSubfields =
        srcSubfieldCode == ANY_CHAR ? sourceField.getSubfields() : sourceField.getSubfields(srcSubfieldCode);

      for (Subfield srcSubfield : srcSubfields) {
        Subfield subfieldToMove =
          srcSubfieldCode == ANY_CHAR ? srcSubfield : marcFactory.newSubfield(newSubfieldCode, srcSubfield.getData());
        newField.addSubfield(subfieldToMove);
      }
      if (!srcSubfields.isEmpty()) {
        addDataFieldInNumericalOrder(newField);
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

    List<DataField> existingFields = marcRecordToChange.getDataFields().stream()
      .filter(field -> fieldMatches(field, existingFieldTag, existingFieldInd1, existingFieldInd2))
      .collect(Collectors.toList());

    if (!existingFields.isEmpty()) {
      for (DataField sourceField : sourceFields) {
        List<Subfield> srcSubfields =
          srcSubfieldCode == ANY_CHAR ? sourceField.getSubfields() : sourceField.getSubfields(srcSubfieldCode);
        for (Subfield srcSubfield : srcSubfields) {
          for (DataField existingField : existingFields) {
            existingField.addSubfield(marcFactory.newSubfield(existingFieldSfCode, srcSubfield.getData()));
          }
        }
        deleteMovedDataFromSourceField(sourceField, srcSubfields, srcSubfieldCode);
      }
    }
  }

  private void deleteMovedDataFromSourceField(DataField sourceField, List<Subfield> movedDataSubfields,
                                              char srcSubfieldCodeFromRule) {
    if (srcSubfieldCodeFromRule == ANY_CHAR || sourceField.getSubfields().size() == movedDataSubfields.size()) {
      marcRecordToChange.removeVariableField(sourceField);
    } else {
      movedDataSubfields.forEach(sourceField::removeSubfield);
    }
  }

  private void replaceAllFields(List<VariableField> fieldReplacements) {
    marcRecordToChange.setLeader(incomingMarcRecord.getLeader());
    for (VariableField field : fieldReplacements) {
      if (Verifier.isControlField(field.getTag())) {
        replaceControlField((ControlField) field);
      } else {
        DataField dataField = (DataField) field;
        replaceDataField(dataField, dataField.getTag(), dataField.getIndicator1(), dataField.getIndicator2(), "*");
      }
    }
    clearUnUpdatedControlFields();
    clearUnUpdatedDataFields();
  }

  private void replaceControlField(ControlField fieldReplacement) {
    boolean fieldsProtected = false;
    List<ControlField> controlFields = marcRecordToChange.getControlFields();

    for (int i = 0; i < controlFields.size(); i++) {
      ControlField fieldToReplace = controlFields.get(i);
      if (fieldToReplace.getTag().equals(fieldReplacement.getTag())) {
        if (isNonRepeatableField(fieldToReplace)) {
          if (isNotProtected(fieldToReplace)) {
            controlFields.set(i, fieldReplacement);
          } else {
            LOGGER.info("replaceControlField:: Field {} was not updated, because it is protected", fieldToReplace);
          }
          return;
        } else {
          if (fieldToReplace.getData().equals(fieldReplacement.getData())) {
            fieldsProtected = true;
            LOGGER.info("replaceControlField:: Field {} was not added, because it is repeatable and contains identical data as an existing one",
              fieldToReplace);
          }
        }
      }
    }

    if (!fieldsProtected) {
      addControlFieldInNumericalOrder(fieldReplacement);
    }
  }

  private boolean isNonRepeatableField(ControlField field) {
    return NON_REPEATABLE_CONTROL_FIELDS_TAGS.contains(field.getTag());
  }

  private void replaceDataField(DataField fieldReplacement, String fieldTag, char ind1, char ind2, String subfieldCode) {
    boolean ifNewDataShouldBeAdded = true;

    List<DataField> dataFields = marcRecordToChange.getDataFields();
    List<DataField> tmpFields = new ArrayList<>();

    if (fieldToRemove == null || !fieldMatches(fieldToRemove, fieldTag, ind1, ind2, subfieldCode.charAt(0))) {
      fieldToRemove = fieldReplacement;
      for (DataField fieldToUpdate : dataFields) {
        if (fieldMatches(fieldToUpdate, fieldTag, ind1, ind2, subfieldCode.charAt(0))) {
          if (isNotProtected(fieldToUpdate)) {
            ifNewDataShouldBeAdded = updateSubfields(subfieldCode, tmpFields, fieldToUpdate, fieldReplacement, ifNewDataShouldBeAdded);
          } else {
            if (isNonRepeatableField(fieldToUpdate)) {
              ifNewDataShouldBeAdded = false;
            }
            LOGGER.info("replaceDataField:: Field {} was not updated, because it is protected", fieldToUpdate);
            doAdditionalProtectedFieldAction(fieldToUpdate);
          }
        }
      }
      tmpFields.removeAll(updatedFields);
      dataFields.removeAll(tmpFields);
    }

    if (ifNewDataShouldBeAdded && !dataFieldsContain(marcRecordToChange.getDataFields(), fieldReplacement)) {
      updatedFields.add(fieldReplacement);
      addDataFieldInNumericalOrder(fieldReplacement);
    }
  }

  protected void doAdditionalProtectedFieldAction(DataField fieldToUpdate) {
    // do nothing
  }

  protected boolean updateSubfields(String subfieldCode, List<DataField> tmpFields, DataField fieldToUpdate,
                                 DataField fieldReplacement, boolean ifNewDataShouldBeAdded) {
    if (subfieldCode.equals(ANY_STRING)) {
      tmpFields.add(fieldToUpdate);
    } else {
      String newSubfieldData = fieldReplacement.getSubfield(subfieldCode.charAt(0)).getData();
      Subfield subfield = fieldToUpdate.getSubfield(subfieldCode.charAt(0));
      if (subfield == null) {
        subfield = new SubfieldImpl(subfieldCode.charAt(0));
        fieldToUpdate.addSubfield(subfield);
        fieldToUpdate.getSubfields()
          .sort(Comparator.<Subfield, Boolean>comparing(sub -> isDigit(sub.getCode()))
            .thenComparing(Subfield::getCode));
      }

      subfield.setData(newSubfieldData);
      ifNewDataShouldBeAdded = false;
      updatedFields.add(fieldToUpdate);
    }

    return ifNewDataShouldBeAdded;
  }

  boolean isNonRepeatableField(DataField field) {
    // is any of 1xx fields
    if (field.getTag().compareTo(TAG_100) > -1 && field.getTag().compareTo(TAG_199) < 1) {
      return true;
    }
    if (field.getTag().equals(TAG_999)) {
      return field.getIndicator1() == INDICATOR_F && field.getIndicator2() == INDICATOR_F;
    }

    return NON_REPEATABLE_DATA_FIELDS_TAGS.contains(field.getTag());
  }

  private void clearUnUpdatedControlFields() {
    List<ControlField> tmpFields = new ArrayList<>();
    for (ControlField controlField : marcRecordToChange.getControlFields()) {
      if (!isControlFieldsContains(incomingMarcRecord.getControlFields(), controlField)
        && isNotProtected(controlField)) {
        tmpFields.add(controlField);
      }
    }
    marcRecordToChange.getControlFields().removeAll(tmpFields);
  }

  private void clearUnUpdatedDataFields() {
    List<DataField> tmpFields = new ArrayList<>();
    for (DataField dataField : marcRecordToChange.getDataFields()) {
      if (unUpdatedFieldShouldBeRemoved(dataField)) {
        tmpFields.add(dataField);
      }
    }
    updatedFields = new ArrayList<>();
    marcRecordToChange.getDataFields().removeAll(tmpFields);
  }

  protected boolean unUpdatedFieldShouldBeRemoved(DataField dataField) {
    return !updatedFields.contains(dataField) && isNotProtected(dataField);
  }

  private boolean isNotProtected(ControlField field) {
    return applicableProtectionSettings.stream()
      .filter(
        setting -> (isBlank(setting.getIndicator1()) && isBlank(setting.getIndicator2()) && isBlank(setting.getSubfield()))
          && setting.getField().equals(ANY_STRING) || setting.getField().equals(field.getTag()))
      .noneMatch(setting -> setting.getData().equals(ANY_STRING) || setting.getData().equals(field.getData()));
  }

  private boolean isNotProtected(DataField field) {
    return applicableProtectionSettings.stream()
      .filter(setting -> setting.getField().equals(ANY_STRING) || setting.getField().equals(field.getTag()))
      .filter(setting -> setting.getIndicator1().equals(ANY_STRING)
        || (isNotEmpty(setting.getIndicator1()) ? setting.getIndicator1().charAt(0) : BLANK_SUBFIELD_CODE)
        == field.getIndicator1())
      .filter(setting -> setting.getIndicator2().equals(ANY_STRING)
        || (isNotEmpty(setting.getIndicator2()) ? setting.getIndicator2().charAt(0) : BLANK_SUBFIELD_CODE)
        == field.getIndicator2())
      .filter(
        setting -> setting.getSubfield().equals(ANY_STRING) || field.getSubfield(setting.getSubfield().charAt(0)) != null)
      .noneMatch(setting -> setting.getData().equals(ANY_STRING) || setting.getData()
        .equals(field.getSubfield(setting.getSubfield().charAt(0)).getData()));
  }

  private boolean isControlFieldsContains(List<ControlField> controlFields, ControlField controlField) {
    return controlFields.stream().anyMatch(field ->
        field.getTag().equals(controlField.getTag())
        && field.getData().equals(controlField.getData()));
  }

  private boolean dataFieldsContain(List<DataField> dataFields, DataField dataField) {
    return dataFields.stream().anyMatch(field -> field.compareTo(dataField) == 0);
  }

}
