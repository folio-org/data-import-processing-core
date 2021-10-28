package org.folio.processing.mapping.defaultmapper.processor.functions;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.folio.AlternativeTitleType;
import org.folio.CallNumberType;
import org.folio.ClassificationType;
import org.folio.ContributorNameType;
import org.folio.ContributorType;
import org.folio.ElectronicAccessRelationship;
import org.folio.HoldingsNoteType;
import org.folio.HoldingsType;
import org.folio.IdentifierType;
import org.folio.InstanceFormat;
import org.folio.InstanceNoteType;
import org.folio.InstanceType;
import org.folio.IssuanceMode;
import org.folio.Location;
import org.folio.processing.mapping.defaultmapper.processor.RuleExecutionContext;
import org.folio.processing.mapping.defaultmapper.processor.functions.enums.CallNumberTypesEnum;
import org.folio.processing.mapping.defaultmapper.processor.functions.enums.ElectronicAccessRelationshipEnum;
import org.folio.processing.mapping.defaultmapper.processor.functions.enums.HoldingsTypeEnum;
import org.folio.processing.mapping.defaultmapper.processor.functions.enums.IssuanceModeEnum;
import org.folio.processing.mapping.defaultmapper.processor.publisher.PublisherRole;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Subfield;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.netty.util.internal.StringUtil.EMPTY_STRING;
import static org.apache.commons.lang3.math.NumberUtils.INTEGER_ZERO;

/**
 * Enumeration to store normalization functions
 */
public enum NormalizationFunction implements Function<RuleExecutionContext, String> {

  CHAR_SELECT() {
    private static final String FROM_PARAMETER = "from";
    private static final String TO_PARAMETER = "to";

    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldValue = context.getSubFieldValue();
      JsonObject ruleParameter = context.getRuleParameter();
      if (ruleParameter != null && ruleParameter.containsKey(FROM_PARAMETER) && ruleParameter.containsKey(TO_PARAMETER)) {
        Integer from = context.getRuleParameter().getInteger(FROM_PARAMETER);
        Integer to = context.getRuleParameter().getInteger(TO_PARAMETER);
        return subFieldValue.substring(from, to);
      } else {
        return subFieldValue;
      }
    }
  },

  REMOVE_ENDING_PUNC() {
    private static final String PUNCT_2_REMOVE = ";:,/+= ";

    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldValue = context.getSubFieldValue();
      if (!StringUtils.isEmpty(subFieldValue)) {
        int lastPosition = subFieldValue.length() - 1;
        if (PUNCT_2_REMOVE.contains(String.valueOf(subFieldValue.charAt(lastPosition)))) {
          return subFieldValue.substring(INTEGER_ZERO, lastPosition);
        }
      }
      return subFieldValue;
    }
  },

  TRIM() {
    @Override
    public String apply(RuleExecutionContext context) {
      return context.getSubFieldValue().trim();
    }
  },

  TRIM_PERIOD() {
    private static final String PERIOD = ".";

    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldData = context.getSubFieldValue();
      if (subFieldData.endsWith(PERIOD)) {
        return subFieldData.substring(INTEGER_ZERO, subFieldData.length() - 1);
      }
      return subFieldData;
    }
  },

  REMOVE_SUBSTRING() {
    private static final String SUBSTRING_PARAMETER = "substring";

    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldValue = context.getSubFieldValue();
      JsonObject ruleParameter = context.getRuleParameter();
      if (ruleParameter != null && ruleParameter.containsKey(SUBSTRING_PARAMETER)) {
        String substring = context.getRuleParameter().getString(SUBSTRING_PARAMETER);
        return StringUtils.remove(subFieldValue, substring);
      } else {
        return subFieldValue;
      }
    }
  },

  REMOVE_PREFIX_BY_INDICATOR() {
    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldData = context.getSubFieldValue();
      DataField dataField = context.getDataField();
      int from = INTEGER_ZERO;
      int to = Character.getNumericValue(dataField.getIndicator2());
      if (0 < to && to < subFieldData.length()) {
        String prefixToRemove = subFieldData.substring(from, to);
        return StringUtils.remove(subFieldData, prefixToRemove);
      } else {
        return subFieldData;
      }
    }
  },

  CAPITALIZE() {
    @Override
    public String apply(RuleExecutionContext context) {
      return StringUtils.capitalize(context.getSubFieldValue());
    }
  },

  CONCAT_SUBFIELDS_BY_NAME() {
    private static final String SUBFIELDS_TO_CONCAT = "subfieldsToConcat";

    @Override
    public String apply(RuleExecutionContext context) {
      StringBuilder subfieldValue = new StringBuilder(context.getSubFieldValue());
      DataField dataField = context.getDataField();
      JsonArray subFields = context.getRuleParameter().getJsonArray(SUBFIELDS_TO_CONCAT);
      int subFieldIndex = IntStream.range(0, dataField.getSubfields().size())
        .filter(i -> dataField.getSubfields().get(i).getData().equals(context.getSubFieldValue()))
        .findFirst()
        .getAsInt();

      return concatSubFields(subFields, dataField, subFieldIndex, subfieldValue);
    }

    private String concatSubFields(JsonArray subFields, DataField dataField, int subFieldIndex, StringBuilder subfieldValue){
      for (int j = 0; j < subFields.size(); j++) {
        String subfieldToAppend = subFields.getString(j);
        String subFieldValueToAppend = dataField.getSubfields().stream()
          .skip(subFieldIndex)
          .filter(e -> String.valueOf(e.getCode()).equals(subfieldToAppend))
          .map(Subfield::getData)
          .findFirst()
          .orElse("");
        subfieldValue.append(" ").append(subFieldValueToAppend);
      }
      return subfieldValue.toString();
    }
  },

  SET_PUBLISHER_ROLE() {
    @Override
    public String apply(RuleExecutionContext context) {
      DataField dataField = context.getDataField();
      int indicator = Character.getNumericValue(dataField.getIndicator2());
      PublisherRole publisherRole = PublisherRole.getByIndicator(indicator);
      if (publisherRole == null) {
        return EMPTY_STRING;
      } else {
        return publisherRole.getCaption();
      }
    }
  },

  SET_INSTANCE_FORMAT_ID() {
    @Override
    public String apply(RuleExecutionContext context) {
      List<InstanceFormat> instanceFormats = context.getMappingParameters().getInstanceFormats();
      if (instanceFormats == null) {
        return StringUtils.EMPTY;
      }
      return instanceFormats.stream()
        .filter(instanceFormat -> instanceFormat.getCode().equalsIgnoreCase(context.getSubFieldValue()))
        .findFirst()
        .map(InstanceFormat::getId)
        .orElse(StringUtils.EMPTY);
    }
  },

  SET_CLASSIFICATION_TYPE_ID() {
    private static final String NAME_PARAMETER = "name";

    @Override
    public String apply(RuleExecutionContext context) {
      String typeName = context.getRuleParameter().getString(NAME_PARAMETER);
      List<ClassificationType> types = context.getMappingParameters().getClassificationTypes();
      if (types == null || typeName == null) {
        return STUB_FIELD_TYPE_ID;
      }
      return types.stream()
        .filter(classificationType -> classificationType.getName().equalsIgnoreCase(typeName))
        .findFirst()
        .map(ClassificationType::getId)
        .orElse(STUB_FIELD_TYPE_ID);
    }
  },

  SET_CONTRIBUTOR_TYPE_ID() {
    @Override
    public String apply(RuleExecutionContext context) {
      List<ContributorType> types = context.getMappingParameters().getContributorTypes();
      if (types == null) {
        return StringUtils.EMPTY;
      }
      return types.stream()
        .filter(type -> type.getCode().equalsIgnoreCase(context.getSubFieldValue()))
        .findFirst()
        .map(ContributorType::getId)
        .orElse(StringUtils.EMPTY);
    }
  },

  SET_CONTRIBUTOR_TYPE_TEXT() {
    @Override
    public String apply(RuleExecutionContext context) {
      List<ContributorType> types = context.getMappingParameters().getContributorTypes();
      if (types == null) {
        return context.getSubFieldValue();
      }
      return types.stream()
        .filter(type -> type.getCode().equalsIgnoreCase(context.getSubFieldValue()))
        .findFirst()
        .map(ContributorType::getName)
        .orElse(context.getSubFieldValue());
    }
  },

  SET_CONTRIBUTOR_NAME_TYPE_ID() {
    private static final String NAME_PARAMETER = "name";

    @Override
    public String apply(RuleExecutionContext context) {
      String typeName = context.getRuleParameter().getString(NAME_PARAMETER);
      List<ContributorNameType> typeNames = context.getMappingParameters().getContributorNameTypes();
      if (typeNames == null || typeName == null) {
        return STUB_FIELD_TYPE_ID;
      }
      return typeNames.stream()
        .filter(contributorTypeName -> contributorTypeName.getName().equalsIgnoreCase(typeName))
        .findFirst()
        .map(ContributorNameType::getId)
        .orElse(STUB_FIELD_TYPE_ID);
    }
  },

  SET_INSTANCE_TYPE_ID() {
    private static final String NAME_PARAMETER = "unspecifiedInstanceTypeCode";

    @Override
    public String apply(RuleExecutionContext context) {
      List<InstanceType> types = context.getMappingParameters().getInstanceTypes();
      if (types == null) {
        return STUB_FIELD_TYPE_ID;
      }
      String unspecifiedTypeCode = context.getRuleParameter().getString(NAME_PARAMETER);
      String instanceTypeValue = context.getDataField() != null ?
        getParsedSubfieldValue(context.getSubFieldValue()) : unspecifiedTypeCode;

      return getInstanceTypeByCode(instanceTypeValue, types)
        .map(InstanceType::getId)
        .orElseGet(() -> getInstanceTypeByCode(unspecifiedTypeCode, types)
          .map(InstanceType::getId)
          .orElse(STUB_FIELD_TYPE_ID));
    }

    private Optional<InstanceType> getInstanceTypeByCode(String instanceTypeValue, List<InstanceType> instanceTypes) {
      return instanceTypes
        .stream()
        .filter(instanceType -> StringUtils.isNotBlank(instanceType.getName()) && StringUtils.isNotBlank(instanceType.getCode()))
        .filter(instanceType ->
          instanceType.getName().equalsIgnoreCase(instanceTypeValue) || instanceType.getCode().equalsIgnoreCase(instanceTypeValue))
        .findFirst();
    }

    private String getParsedSubfieldValue(String subfieldValue) {
      String[] subfields = subfieldValue.split("~");
      return subfields[subfields.length - 1];
    }
  },

  SET_ELECTRONIC_ACCESS_RELATIONS_ID() {
    @Override
    public String apply(RuleExecutionContext context) {
      List<ElectronicAccessRelationship> electronicAccessRelationships = context.getMappingParameters().getElectronicAccessRelationships();
      if (electronicAccessRelationships == null || context.getDataField() == null) {
        return STUB_FIELD_TYPE_ID;
      }
      char ind2 = context.getDataField().getIndicator2();
      String name = ElectronicAccessRelationshipEnum.getNameByIndicator(ind2);
      return electronicAccessRelationships
        .stream()
        .filter(electronicAccessRelationship -> electronicAccessRelationship.getName().equalsIgnoreCase(name))
        .findFirst()
        .map(ElectronicAccessRelationship::getId)
        .orElse(STUB_FIELD_TYPE_ID);
    }
  },

  SET_IDENTIFIER_TYPE_ID_BY_NAME() {
    private static final String NAME_PARAMETER = "name";

    @Override
    public String apply(RuleExecutionContext context) {
      String typeName = context.getRuleParameter().getString(NAME_PARAMETER);
      List<IdentifierType> identifierTypes = context.getMappingParameters().getIdentifierTypes();
      if (identifierTypes == null || typeName == null) {
        return STUB_FIELD_TYPE_ID;
      }
      return identifierTypes.stream()
        .filter(identifierType -> identifierType.getName().trim().equalsIgnoreCase(typeName))
        .findFirst()
        .map(IdentifierType::getId)
        .orElse(STUB_FIELD_TYPE_ID);
    }
  },

  SET_IDENTIFIER_TYPE_ID_BY_VALUE() {
    private static final String NAMES_PARAMETER = "names";
    private static final String OCLC_REGEX = "oclc_regex";

    @Override
    public String apply(RuleExecutionContext context) {
      JsonArray typeNames = context.getRuleParameter().getJsonArray(NAMES_PARAMETER);
      List<IdentifierType> identifierTypes = context.getMappingParameters().getIdentifierTypes();
      if (identifierTypes == null || typeNames == null) {
        return STUB_FIELD_TYPE_ID;
      }
      String type = getIdentifierTypeName(context);
      return identifierTypes.stream()
        .filter(identifierType -> identifierType.getName().equalsIgnoreCase(type))
        .findFirst()
        .map(IdentifierType::getId)
        .orElse(STUB_FIELD_TYPE_ID);
    }

    private String getIdentifierTypeName(RuleExecutionContext context) {
      JsonArray typeNames = context.getRuleParameter().getJsonArray(NAMES_PARAMETER);
      String oclcRegex = context.getRuleParameter().getString(OCLC_REGEX);
      String type = typeNames.getString(0);
      if (oclcRegex != null && context.getSubFieldValue().matches(oclcRegex)) {
        type = typeNames.getString(1);
      }
      return type;
    }
  },

  SET_NOTE_TYPE_ID() {
    private static final String NAME_PARAMETER = "name";
    private static final String DEFAULT_NOTE_TYPE_NAME = "General note";

    @Override
    public String apply(RuleExecutionContext context) {
      String noteTypeName = context.getRuleParameter().getString(NAME_PARAMETER);
      List<InstanceNoteType> instanceNoteTypes = context.getMappingParameters().getInstanceNoteTypes();
      if (instanceNoteTypes == null || noteTypeName == null) {
        return STUB_FIELD_TYPE_ID;
      }
      return getNoteTypeByName(noteTypeName, instanceNoteTypes)
        .map(InstanceNoteType::getId)
        .orElseGet(() -> getNoteTypeByName(DEFAULT_NOTE_TYPE_NAME, instanceNoteTypes)
          .map(InstanceNoteType::getId)
          .orElse(STUB_FIELD_TYPE_ID));
    }

    private Optional<InstanceNoteType> getNoteTypeByName(String noteTypeName, List<InstanceNoteType> noteTypes) {
      return noteTypes
        .stream()
        .filter(instanceNoteType -> instanceNoteType.getName().equalsIgnoreCase(noteTypeName))
        .findFirst();
    }
  },

  SET_ALTERNATIVE_TITLE_TYPE_ID() {
    private static final String NAME_PARAMETER = "name";

    @Override
    public String apply(RuleExecutionContext context) {
      String alternativeTitleTypeName = context.getRuleParameter().getString(NAME_PARAMETER);
      List<AlternativeTitleType> alternativeTitleTypes = context.getMappingParameters().getAlternativeTitleTypes();
      if (alternativeTitleTypes == null || alternativeTitleTypeName == null) {
        return STUB_FIELD_TYPE_ID;
      }
      return alternativeTitleTypes.stream()
        .filter(alternativeTitleType -> alternativeTitleType.getName().equalsIgnoreCase(alternativeTitleTypeName))
        .findFirst()
        .map(AlternativeTitleType::getId)
        .orElse(STUB_FIELD_TYPE_ID);
    }
  },

  SET_ISSUANCE_MODE_ID() {
    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldValue = context.getSubFieldValue();
      char seventhChar = subFieldValue.charAt(7); //Regarding "MODSOURMAN-203" is should be 7-th symbol.
      List<IssuanceMode> issuanceModes = context.getMappingParameters().getIssuanceModes();
      if (issuanceModes == null || issuanceModes.isEmpty()) {
        return StringUtils.EMPTY;
      }
      String defaultIssuanceModeId = findIssuanceModeId(issuanceModes, IssuanceModeEnum.UNSPECIFIED, StringUtils.EMPTY);
      return matchIssuanceModeIdViaLeaderSymbol(seventhChar, issuanceModes, defaultIssuanceModeId);
    }

    private String findIssuanceModeId(List<IssuanceMode> issuanceModes, IssuanceModeEnum issuanceModeType,
                                      String defaultId) {
      return issuanceModes.stream()
        .filter(issuanceMode -> issuanceMode.getName().equalsIgnoreCase(issuanceModeType.getValue()))
        .findFirst()
        .map(IssuanceMode::getId)
        .orElse(defaultId);
    }

    private String matchIssuanceModeIdViaLeaderSymbol(char seventhChar, List<IssuanceMode> issuanceModes, String defaultId) {
      IssuanceModeEnum issuanceMode = matchSymbolToIssuanceMode(seventhChar);
      return findIssuanceModeId(issuanceModes, issuanceMode, defaultId);
    }
  },

  SET_NOTE_STAFF_ONLY_VIA_INDICATOR() {
    @Override
    public String apply(RuleExecutionContext context) {
      DataField dataField = context.getDataField();
      char firstIndicator = dataField.getIndicator1();
      if (firstIndicator == '0') {
        return "true";
      }
      return "false";
    }
  },

  SET_HOLDINGS_TYPE_ID() {
    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldValue = context.getSubFieldValue();
      char sixthChar = subFieldValue.charAt(6);
      List<HoldingsType> holdingsTypes = context.getMappingParameters().getHoldingsTypes();
      if (holdingsTypes == null || holdingsTypes.isEmpty()) {
        return STUB_FIELD_TYPE_ID;
      }
      String marcHoldingsType = HoldingsTypeEnum.getNameByCharacter(sixthChar);
      return findHoldingsTypeId(holdingsTypes, marcHoldingsType);
    }

    private String findHoldingsTypeId(List<HoldingsType> holdingsTypes, String marcHoldingsType) {
      return holdingsTypes.stream()
        .filter(holdingsType -> holdingsType.getName().equalsIgnoreCase(marcHoldingsType))
        .findFirst()
        .map(HoldingsType::getId)
        .orElse(STUB_FIELD_TYPE_ID);
    }
  },

  SET_CALL_NUMBER_TYPE_ID() {
    @Override
    public String apply(RuleExecutionContext context) {
      List<CallNumberType> callNumberTypes = context.getMappingParameters().getCallNumberTypes();
      if (callNumberTypes == null || context.getDataField() == null) {
        return StringUtils.EMPTY;
      }
      char ind1 = context.getDataField().getIndicator1();
      String name = CallNumberTypesEnum.getNameByIndicator(ind1);
      return callNumberTypes
        .stream()
        .filter(callNumberType -> callNumberType.getName().equalsIgnoreCase(name))
        .findFirst()
        .map(CallNumberType::getId)
        .orElse(StringUtils.EMPTY);
    }
  },

  SET_PERMANENT_LOCATION_ID() {
    @Override
    public String apply(RuleExecutionContext context) {
      var locations = context.getMappingParameters().getLocations();
      if (locations == null || context.getDataField() == null) {
        return STUB_FIELD_TYPE_ID;
      }
      var subFieldValue = context.getSubFieldValue();
      return locations.stream()
        .filter(location -> location.getCode().equals(subFieldValue))
        .findFirst()
        .map(Location::getId)
        .orElse(STUB_FIELD_TYPE_ID);
    }
  },

  SET_HOLDINGS_NOTE_TYPE_ID() {
    private static final String NAME_PARAMETER = "name";

    @Override
    public String apply(RuleExecutionContext context) {
      String noteTypeName = context.getRuleParameter().getString(NAME_PARAMETER);
      List<HoldingsNoteType> holdingsNoteTypes = context.getMappingParameters().getHoldingsNoteTypes();
      if (holdingsNoteTypes == null || noteTypeName == null) {
        return STUB_FIELD_TYPE_ID;
      }
      return holdingsNoteTypes
        .stream()
        .filter(holdingsNoteType -> holdingsNoteType.getName().equalsIgnoreCase(noteTypeName))
        .map(HoldingsNoteType::getId).collect(Collectors.joining());
    }
  };

  public IssuanceModeEnum matchSymbolToIssuanceMode(char symbol) {
    for (IssuanceModeEnum issuanceMode : IssuanceModeEnum.values()) {
      for (int i = 0; i < issuanceMode.getSymbols().length; i++) {
        if (issuanceMode.getSymbols()[i] == symbol) {
          return issuanceMode;
        }
      }
    }
    return IssuanceModeEnum.UNSPECIFIED;
  }

  private static final String STUB_FIELD_TYPE_ID = "fe19bae4-da28-472b-be90-d442e2428ead";
}
