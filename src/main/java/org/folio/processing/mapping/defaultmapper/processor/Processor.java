package org.folio.processing.mapping.defaultmapper.processor;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.LinkedHashMap;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.AuthorityExtended;
import org.folio.processing.mapping.defaultmapper.processor.functions.NormalizationFunctionRunner;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.defaultmapper.processor.util.ExtraFieldUtil;
import org.marc4j.MarcJsonReader;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Leader;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.impl.SubfieldImpl;

import javax.script.ScriptException;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang.StringUtils.EMPTY;
import static org.folio.processing.mapping.defaultmapper.processor.LoaderHelper.isMappingValid;
import static org.folio.processing.mapping.defaultmapper.processor.LoaderHelper.isPrimitiveOrPrimitiveWrapperOrString;

public class Processor<T> {

  private static final Logger LOGGER = LogManager.getLogger(Processor.class);
  private static final String VALUE = "value";
  private static final String CUSTOM = "custom";
  private static final String TYPE = "type";
  private static final String REPEATABLE_SUBFIELD_SEPARATOR = StringUtils.SPACE;
  private static final String INDICATORS = "indicators";
  private static final String IND_1 = "ind1";
  private static final String IND_2 = "ind2";
  private static final String WILDCARD_INDICATOR = "*";
  private static final String TARGET = "target";
  private static final String SUBFIELD = "subfield";
  private static final String CREATE_SINGLE_OBJECT_PROPERY = "createSingleObject";
  private static final String RULES = "rules";
  private static final Map<Class<?>, Map<String, Field>> FIELD_CACHE = new ConcurrentHashMap<>();
  private static final Map<Class<?>, Map<String, Method>> METHOD_CACHE = new ConcurrentHashMap<>();
  private static final Map<Field, ParameterizedType> PARAM_TYPE_CACHE = new ConcurrentHashMap<>();
  public static final String ALTERNATIVE_MAPPING = "alternativeMapping";
  private static final String FIELDS_WITH_TRUNCATED_MAPPING_POSTFIX = "Trunc";
  private static final String SAFT_FIELDS_PREFIX = "saft";
  public static final String DELIMITER_SUBFIELDS = "subfields";
  public static final String LDR_TAG = "LDR";

  private JsonObject mappingRules;
  private Leader leader;
  private String separator; //separator between subfields with different delimiters
  private JsonArray delimiters;
  private T entity;
  private JsonArray rules;
  private boolean createNewComplexObj;
  private boolean entityRequested;
  private boolean entityRequestedPerRepeatedSubfield;
  private boolean keepTrailingBackslash;
  private final List<StringBuilder> buffers2concat = new ArrayList<>();
  private final Map<String, StringBuilder> subField2Data = new HashMap<>();
  private final Map<String, String> subField2Delimiter = new HashMap<>();
  private final Set<String> ignoredSubsequentFields = new HashSet<>();
  private final Set<Character> ignoredSubsequentSubfields = new HashSet<>();

  public T process(JsonObject record, MappingParameters mappingParameters, JsonObject mappingRules, Class<T> entityClass) {
    entity = null;
    try {
      this.mappingRules = checkNotNull(mappingRules);
      final MarcJsonReader reader = new MarcJsonReader(new ByteArrayInputStream(record.toString().getBytes(UTF_8)));
      if (reader.hasNext()) {
        Record marcRecord = reader.next();
        entity = processSingleEntry(marcRecord, mappingParameters, entityClass);
      }
    } catch (Exception e) {
      LOGGER.warn("process:: Error mapping Marc record: {}", record.encode(), e);
    }
    return entity;
  }

  private T processSingleEntry(Record record, MappingParameters mappingParameters, Class<T> entityClass) {
    try {
      var entityClassConstructor = entityClass.getConstructor();
      this.entity = entityClassConstructor.newInstance();
      var setIdMethod = entityClass.getMethod("setId", String.class);
      setIdMethod.invoke(entity, UUID.randomUUID().toString());
      leader = record.getLeader();
      processLeaderField(leader, mappingParameters);
      processControlFieldSection(record.getControlFields().iterator(), mappingParameters);
      processDataFieldSection(record.getDataFields().iterator(), mappingParameters);
      return this.entity;
    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
      return null;
    }
  }

  private void processLeaderField(Leader leader, MappingParameters mappingParameters)
    throws InstantiationException, IllegalAccessException {
    if (leader != null) {
      JsonArray leaderRules = mappingRules.getJsonArray(LDR_TAG);
      if (leaderRules != null) {
        handleFieldRules(leaderRules, leader.toString(), mappingParameters);
      }
    }
  }

  private void handleFieldRules(JsonArray fieldRules, String field, MappingParameters mappingParameters)
    throws InstantiationException, IllegalAccessException {

    Object[] rememberComplexObj = new Object[]{null};
    createNewComplexObj = true;

    for (int i = 0; i < fieldRules.size(); i++) {
      JsonObject rule = fieldRules.getJsonObject(i);
      rules = rule.getJsonArray(RULES);

      RuleExecutionContext ruleExecutionContext = new RuleExecutionContext();
      ruleExecutionContext.setMappingParameters(mappingParameters);
      ruleExecutionContext.setSubFieldValue(field);

      String data = processRules(ruleExecutionContext);
      if (data.isEmpty()) {
        continue;
      }

      String target = rule.getString(TARGET);
      String[] embeddedFields = target.split("\\.");

      if (BooleanUtils.isTrue(rule.getBoolean(CREATE_SINGLE_OBJECT_PROPERY))) {
        if (data.isBlank()) {
          data = null;
        }
        buildAndFillSimpleObject(entity, embeddedFields, data);
        createNewComplexObj = false;
      } else {
        if (isMappingValid(entity, embeddedFields)) {
          Object val = getValue(entity, embeddedFields, data);
          buildObject(entity, embeddedFields, createNewComplexObj, val, rememberComplexObj);
          createNewComplexObj = false;
        } else {
          LOGGER.warn("handleFieldRules:: bad mapping {}", rules.encode());
        }
      }
    }
  }

  private void processDataFieldSection(Iterator<DataField> dfIter, MappingParameters mappingParameters) throws IllegalAccessException, ScriptException,
    InstantiationException {

    while (dfIter.hasNext()) {
      DataField dataField = dfIter.next();
      ExtraFieldUtil.findAndReplaceFieldsIfNeed(dataField, mappingRules);
      RuleExecutionContext ruleExecutionContext = new RuleExecutionContext();
      ruleExecutionContext.setMappingParameters(mappingParameters);
      ruleExecutionContext.setDataField(dataField);
      handleRecordDataFieldByField(ruleExecutionContext);
    }
  }

  private void handleRecordDataFieldByField(RuleExecutionContext ruleExecutionContext) throws ScriptException, IllegalAccessException,
    InstantiationException {
    DataField dataField = ruleExecutionContext.getDataField();
    createNewComplexObj = true; // each rule will generate a new instance in an array , for an array data member
    Object[] rememberComplexObj = new Object[]{null};
    JsonArray mappingEntry = getDataFieldMapping(dataField);
    if (mappingEntry == null) {
      return;
    }
    JsonArray fieldMappingIndicators = getFieldMappingIndicators(mappingEntry);

    //there is a mapping associated with this marc field
    for (int i = 0; i < mappingEntry.size(); i++) {
      //there could be multiple mapping entries, specifically different mappings
      //per subfield in the marc field
      JsonObject subFieldMapping = mappingEntry.getJsonObject(i);
      //check if mapping entry has indicators sets
      if (!fieldMappingIndicators.isEmpty()) {
        String dataFieldInd1 = String.valueOf(dataField.getIndicator1());
        String dataFieldInd2 = String.valueOf(dataField.getIndicator2());
        if (subFieldMapping.containsKey(INDICATORS)) {
          if (!checkOnIndicatorsCorrespondence(subFieldMapping, dataFieldInd1, dataFieldInd2)) {
            continue;
          }
        } else {
          if (checkOnIndicatorsMatches(fieldMappingIndicators, dataFieldInd1, dataFieldInd2)) {
            continue;
          }
        }
      }
      if (canProcessSubFieldMapping(subFieldMapping, dataField)) {
        processSubFieldMapping(subFieldMapping, rememberComplexObj, ruleExecutionContext);
      }
    }
  }

  private JsonArray getDataFieldMapping(DataField dataField) {
    JsonArray mappingArray = mappingRules.getJsonArray(dataField.getTag());
    if (entity instanceof AuthorityExtended) {
      addSubFieldDelimiterForAuthorities(dataField, mappingArray);
      return addExtraMappingsForAuthorities(dataField, mappingArray);
    }
    return mappingArray;
  }

  private boolean canProcessSubFieldMapping(JsonObject subFieldMapping, DataField dataField) {
    if (subFieldMapping.containsKey("ignoreSubsequentFields")) {
      boolean mapFirstFieldOccurrence = subFieldMapping.getBoolean("ignoreSubsequentFields");
      if (mapFirstFieldOccurrence) {
        if (ignoredSubsequentFields.contains(dataField.getTag())) {
          return false;
        } else {
          ignoredSubsequentFields.add(dataField.getTag());
        }
      }
    }
    return true;
  }

  private JsonArray getFieldMappingIndicators(JsonArray mappingEntry) {
    JsonArray indicatorsArray = new JsonArray();
    for (int i = 0; i < mappingEntry.size(); i++) {
      if (mappingEntry.getJsonObject(i).containsKey(INDICATORS)) {
        indicatorsArray.add(mappingEntry.getJsonObject(i).getJsonObject(INDICATORS));
      }
    }
    return indicatorsArray;
  }

  private boolean checkOnIndicatorsCorrespondence(JsonObject subFieldMapping, String dataFieldInd1, String dataFieldInd2) {
    String subFieldMappingInd1 = subFieldMapping.getJsonObject(INDICATORS).getString(IND_1);
    String subFieldMappingInd2 = subFieldMapping.getJsonObject(INDICATORS).getString(IND_2);

    return (dataFieldInd1.equals(subFieldMappingInd1) || WILDCARD_INDICATOR.equals(subFieldMappingInd1))
      && (dataFieldInd2.equals(subFieldMappingInd2) || WILDCARD_INDICATOR.equals(subFieldMappingInd2));
  }

  private boolean checkOnIndicatorsMatches(JsonArray fieldMappingIndicators, String dataFieldInd1, String dataFieldInd2) {
    for (int i = 0; i < fieldMappingIndicators.size(); i++) {
      JsonObject indicatorsObj = fieldMappingIndicators.getJsonObject(i);
      String subFieldMappingInd1 = indicatorsObj.getString(IND_1);
      String subFieldMappingInd2 = indicatorsObj.getString(IND_2);
      if (dataFieldInd1.equals(subFieldMappingInd1) || WILDCARD_INDICATOR.equals(subFieldMappingInd1)
        && (dataFieldInd2.equals(subFieldMappingInd2) || WILDCARD_INDICATOR.equals(subFieldMappingInd2))) {
        return true;
      }
    }
    return false;
  }

  private void processSubFieldMapping(JsonObject subFieldMapping, Object[] rememberComplexObj, RuleExecutionContext ruleExecutionContext)
    throws IllegalAccessException, InstantiationException, ScriptException {

    //a single mapping entry can also map multiple subfields to a specific field in the instance
    JsonArray mappingRuleEntry = subFieldMapping.getJsonArray("entity");

    //entity field indicates that the subfields within the entity definition should be
    //a single instance, anything outside the entity definition will be placed in another
    //instance of the same type, unless the target points to a different type.
    //multiple entities can be declared in a field, meaning each entity will be a new instance
    //with the subfields defined in a single entity grouped as a single instance.
    //all definitions not enclosed within the entity will be associated with anothe single instance
    entityRequested = false;

    //for repeatable subfields, you can indicate that each repeated subfield should respect
    //the new instance declaration and create a new instance. so that if there are two "a" subfields
    //each one will create its own instance
    entityRequestedPerRepeatedSubfield = BooleanUtils.isTrue(subFieldMapping.getBoolean(
      "entityPerRepeatedSubfield"));

    //for subfields there could be the case when you need to keep trailing backslash instead of removing it
    keepTrailingBackslash = BooleanUtils.isTrue(subFieldMapping.getBoolean("keepTrailingBackslash"));

    //if no "entity" is defined , then all rules contents of the field getting mapped to the same type
    //will be placed in a single instance of that type.
    if (mappingRuleEntry == null) {
      mappingRuleEntry = new JsonArray();
      mappingRuleEntry.add(subFieldMapping);
    } else {
      entityRequested = true;
    }

    List<Object[]> arraysOfObjects = new ArrayList<>();
    for (int i = 0; i < mappingRuleEntry.size(); i++) {
      DataField dataField = ruleExecutionContext.getDataField();
      JsonObject fieldRule = mappingRuleEntry.getJsonObject(i);
      if (!recordHasAllRequiredSubfields(dataField, fieldRule)
        || recordHasExclusiveSubfields(dataField, fieldRule)) {
        ignoredSubsequentSubfields.clear();
        return;
      }
      handleFields(fieldRule, arraysOfObjects, rememberComplexObj, ruleExecutionContext);
    }

    if (entityRequested) {
      createNewComplexObj = true;
    }
    ignoredSubsequentSubfields.clear();
  }

  /**
   * Method checks if record field contains all required sub-fields (that come from mapping rules).
   *
   * @param recordDataField data field from record
   * @param fieldRule       mapping configuration rule for specific field
   * @return If there is required sub-fields in mapping rules, then method checks if record field contains all of them.
   * If there is no required sub-fields in mapping rules, method just returns true
   */
  private boolean recordHasAllRequiredSubfields(DataField recordDataField, JsonObject fieldRule) {
    if (fieldRule.containsKey("requiredSubfield")) {
      List<String> requiredSubFieldsFromMapping = fieldRule.getJsonArray("requiredSubfield").getList();
      Set<String> subFieldsFromRecord = recordDataField.getSubfields()
        .stream()
        .map(subField -> String.valueOf(subField.getCode()))
        .collect(Collectors.toSet());
      return subFieldsFromRecord.containsAll(requiredSubFieldsFromMapping);
    }
    return true;
  }

  /**
   * Method checks if record field contains any exclusive sub-field (that come from mapping rules).
   *
   * @param recordDataField data field from record
   * @param fieldRule       mapping configuration rule for specific field
   * @return If there is exclusive sub-fields in mapping rules, then method checks if record field contains any of them.
   * If there is no exclusive sub-fields in mapping rules, method just returns false
   */
  private boolean recordHasExclusiveSubfields(DataField recordDataField, JsonObject fieldRule) {
    if (fieldRule.containsKey("exclusiveSubfield")) {
      List<String> exclusiveSubfieldsFromMapping = fieldRule.getJsonArray("exclusiveSubfield").getList();
      Set<String> subFieldsFromRecord = recordDataField.getSubfields()
        .stream()
        .map(subField -> String.valueOf(subField.getCode()))
        .collect(Collectors.toSet());
      return subFieldsFromRecord.stream().anyMatch(exclusiveSubfieldsFromMapping::contains);
    }
    return false;
  }

  private void handleFields(JsonObject jObj,
                            List<Object[]> arraysOfObjects,
                            Object[] rememberComplexObj,
                            RuleExecutionContext ruleExecutionContext)
    throws ScriptException, IllegalAccessException, InstantiationException {

    //push into a set so that we can do a lookup for each subfield in the marc instead
    //of looping over the array
    Set<String> subFieldsSet = jObj.getJsonArray(SUBFIELD).stream()
      .filter(o -> o instanceof String)
      .map(o -> (String) o)
      .collect(Collectors.toCollection(HashSet::new));

    //it can be a one to one mapping, or there could be rules to apply prior to the mapping
    rules = jObj.getJsonArray(RULES);

    // see ### Delimiters in README.md (section Processor.java)
    delimiters = jObj.getJsonArray("subFieldDelimiter");

    //this is a map of each subfield to the delimiter to delimit it with
    subField2Delimiter.clear();

    //should we run rules on each subfield value independently or on the entire concatenated
    //string, not relevant for non repeatable single subfield declarations or entity declarations
    //with only one non repeatable subfield
    boolean applyPost = false;

    if (jObj.getBoolean("applyRulesOnConcatenatedData") != null) {
      applyPost = jObj.getBoolean("applyRulesOnConcatenatedData");
    }

    //map a subfield to a stringbuilder which will hold its content
    //since subfields can be concatenated into the same stringbuilder
    //the map of different subfields can map to the same stringbuilder reference
    subField2Data.clear();

    //keeps a reference to the stringbuilders that contain the data of the
    //subfield sets. this list is then iterated over and used to delimit subfield sets
    buffers2concat.clear();

    handleDelimiters();

    String[] embeddedFields = jObj.getString(TARGET).split("\\.");


    if (!isMappingValid(entity, embeddedFields)) {
      LOGGER.debug("handleFields:: bad mapping {}", jObj::encode);
      return;
    }

    //iterate over the subfields in the mapping entry
    List<Subfield> subFields = ruleExecutionContext.getDataField().getSubfields();

    //check if we need to expand the subfields into additional subfields
    JsonObject splitter = jObj.getJsonObject("subFieldSplit");
    if (splitter != null) {
      expandSubfields(subFields, splitter);
    }
    if (subFields.stream().noneMatch(sf -> (checkIfSubfieldShouldBeHandled(subFieldsSet, sf)))) {
      //skip further processing if there are no subfields to map
      LOGGER.debug("handleFields:: no subfields to map from {} to {}", subFields.stream().map(Subfield::getCode).toList(), subFieldsSet);
      return;
    }

    for (int i = 0; i < subFields.size(); i++) {
      //check if there are no mapped elements present
      if (checkIfSubfieldShouldBeHandled(subFieldsSet, subFields.get(i)) && canHandleSubField(subFields.get(i), jObj)) {
        handleSubFields(ruleExecutionContext, subFields, i, subFieldsSet, arraysOfObjects, applyPost, embeddedFields);
      }
    }

    if (!(entityRequestedPerRepeatedSubfield && entityRequested)) {

      String completeData = generateDataString();
      if (applyPost) {
        ruleExecutionContext.setSubFieldValue(completeData);
        completeData = processRules(ruleExecutionContext);
      }
      if (createNewObject(embeddedFields, completeData, rememberComplexObj)) {
        createNewComplexObj = false;
      }

      if (StringUtils.isEmpty(completeData) && jObj.containsKey(ALTERNATIVE_MAPPING)) {
        ignoredSubsequentSubfields.clear();
        handleFields(jObj.getJsonObject(ALTERNATIVE_MAPPING), arraysOfObjects, rememberComplexObj, ruleExecutionContext);
      }
    }
  }

  private boolean canHandleSubField(Subfield subfield, JsonObject mappingRuleEntry) {
    if (mappingRuleEntry.containsKey("ignoreSubsequentSubfields")) {
      boolean mapFirstSubfieldOccurrence = mappingRuleEntry.getBoolean("ignoreSubsequentSubfields");
      if (mapFirstSubfieldOccurrence) {
        if (ignoredSubsequentSubfields.contains(subfield.getCode())) {
          return false;
        } else {
          ignoredSubsequentSubfields.add(subfield.getCode());
        }
      }
    }
    return true;
  }

  private void handleSubFields(RuleExecutionContext ruleExecutionContext, List<Subfield> subFields, int subFieldsIndex, Set<String> subFieldsSet,
                               List<Object[]> arraysOfObjects, boolean applyPost, String[] embeddedFields) {

    String data = subFields.get(subFieldsIndex).getData();
    char sub1 = subFields.get(subFieldsIndex).getCode();
    String subfield = String.valueOf(sub1);
    if (!subFieldsSet.contains(subfield)) {
      return;
    }

    //rule file contains a rule for this subfield
    if (arraysOfObjects.size() <= subFieldsIndex) {
      temporarilySaveObjectsWithMultipleFields(arraysOfObjects, subFieldsIndex);
    }

    if (!applyPost) {

      //apply rule on the per subfield data. if applyPost is set to true, we need
      //to wait and run this after all the data associated with this target has been
      //concatenated , therefore this can only be done in the createNewObject function
      //which has the full set of subfield data
      ruleExecutionContext.setSubFieldValue(data);
      data = processRules(ruleExecutionContext);
    }

    if (delimiters != null && subField2Data.get(subfield) != null) {
      //delimiters is not null, meaning we have a string buffer for each set of subfields
      //so populate the appropriate string buffer
      if (!subField2Data.get(subfield).isEmpty()) {
        subField2Data.get(subfield).append(subField2Delimiter.get(subfield));
      }
      subField2Data.get(subfield).append(data);
    } else {
      StringBuilder sb = buffers2concat.get(0);
      if (entityRequestedPerRepeatedSubfield) {
        //create a new value no matter what , since this use case
        //indicates that repeated and non-repeated subfields will create a new entity
        //so we should not concat values
        sb.delete(0, sb.length());
      }
      if (!sb.isEmpty()) {
        sb.append(REPEATABLE_SUBFIELD_SEPARATOR);
      }
      sb.append(data);
    }

    if (entityRequestedPerRepeatedSubfield && entityRequested) {
      createNewComplexObj = arraysOfObjects.get(subFieldsIndex)[0] == null;
      String completeData = generateDataString();
      createNewObject(embeddedFields, completeData, arraysOfObjects.get(subFieldsIndex));
    }
  }

  private void temporarilySaveObjectsWithMultipleFields(List<Object[]> arraysOfObjects, int subFieldsIndex) {
    //temporarily save objects with multiple fields so that the fields of the
    //same instance can be populated with data from different subfields
    for (int i = arraysOfObjects.size(); i <= subFieldsIndex; i++) {
      arraysOfObjects.add(new Object[]{null});
    }
  }

  private void handleDelimiters() {

    if (delimiters != null) {

      for (int i = 0; i < delimiters.size(); i++) {
        JsonObject job = delimiters.getJsonObject(i);
        String delimiter = job.getString(VALUE);
        JsonArray subFieldswithDel = job.getJsonArray(DELIMITER_SUBFIELDS);
        StringBuilder subFieldsStringBuilder = new StringBuilder();
        buffers2concat.add(subFieldsStringBuilder);
        if (subFieldswithDel.isEmpty()) {
          separator = delimiter;
        }

        for (int ii = 0; ii < subFieldswithDel.size(); ii++) {
          subField2Delimiter.put(subFieldswithDel.getString(ii), delimiter);
          subField2Data.put(subFieldswithDel.getString(ii), subFieldsStringBuilder);
        }
      }
    } else {
      buffers2concat.add(new StringBuilder());
    }
  }

  private void processControlFieldSection(Iterator<ControlField> ctrlIter, MappingParameters context)
    throws IllegalAccessException, InstantiationException {

    //iterate over all the control fields in the marc record
    //for each control field , check if there is a rule for mapping that field in the rule file
    while (ctrlIter.hasNext()) {
      ControlField controlField = ctrlIter.next();
      //get entry for this control field in the rules.json file
      JsonArray controlFieldRules = mappingRules.getJsonArray(controlField.getTag());
      if (controlFieldRules != null) {
        handleFieldRules(controlFieldRules, controlField.getData(), context);
      }
    }
  }

  private String processRules(RuleExecutionContext ruleExecutionContext) {
    if (rules == null) {
      return Escaper.escape(ruleExecutionContext.getSubFieldValue(), keepTrailingBackslash)
        .replaceAll("\\\\\"", "\"");
    }

    //there are rules associated with this subfield / control field - to instance field mapping
    String originalData = ruleExecutionContext.getSubFieldValue();
    for (int i = 0; i < rules.size(); i++) {
      ProcessedSingleItem psi = processRule(rules.getJsonObject(i), ruleExecutionContext, originalData);
      ruleExecutionContext.setSubFieldValue(psi.getData());
      if (psi.doBreak()) {
        break;
      }
    }
    return Escaper.escape(ruleExecutionContext.getSubFieldValue(), keepTrailingBackslash)
      .replaceAll("\\\\\"", "\"");
  }

  private ProcessedSingleItem processRule(JsonObject rule, RuleExecutionContext ruleExecutionContext, String originalData) {


    //get the conditions associated with each rule
    JsonArray conditions = rule.getJsonArray("conditions");

    // see ### constant value in README.md (section Processor.java)
    String ruleConstVal = rule.getString(VALUE);
    boolean conditionsMet = true;

    //each rule has conditions, if they are all met, then mark
    //continue processing the next condition, if all conditions are met
    //set the target to the value of the rule
    boolean isCustom = false;
    for (int m = 0; m < conditions.size(); m++) {
      JsonObject condition = conditions.getJsonObject(m);

      // see ### functions in README.md (section Processor.java)
      String[] functions = ProcessorHelper.getFunctionsFromCondition(condition);
      isCustom = checkIfAnyFunctionIsCustom(functions, isCustom);

      ProcessedSinglePlusConditionCheck processedCondition =
        processCondition(condition, ruleExecutionContext, originalData, conditionsMet, ruleConstVal, isCustom);
      ruleExecutionContext.setSubFieldValue(processedCondition.getData());
      conditionsMet = processedCondition.isConditionsMet();
    }

    if (conditionsMet && ruleConstVal != null && !isCustom) {

      //all conditions of the rule were met, and there
      //is a constant value associated with the rule, and this is
      //not a custom rule, then set the data to the const value
      //no need to continue processing other rules for this subfield
      return new ProcessedSingleItem(ruleConstVal, true);
    }
    return new ProcessedSingleItem(ruleExecutionContext.getSubFieldValue(), false);
  }

  private ProcessedSinglePlusConditionCheck processCondition(JsonObject condition, RuleExecutionContext ruleExecutionContext, String originalData,
                                                             boolean conditionsMet, String ruleConstVal,
                                                             boolean isCustom) {
    String valueParam = condition.getString(VALUE);
    for (String function : ProcessorHelper.getFunctionsFromCondition(condition)) {
      ProcessedSinglePlusConditionCheck processedFunction = processFunction(function, ruleExecutionContext, isCustom, valueParam, condition,
        conditionsMet, ruleConstVal);
      conditionsMet = processedFunction.isConditionsMet();
      ruleExecutionContext.setSubFieldValue(processedFunction.getData());
      if (processedFunction.doBreak()) {
        break;
      }
    }

    if (!conditionsMet) {

      //all conditions for this rule we not met, revert data to the originalData passed in.
      return new ProcessedSinglePlusConditionCheck(originalData, true, false);
    }
    return new ProcessedSinglePlusConditionCheck(ruleExecutionContext.getSubFieldValue(), false, true);
  }

  private ProcessedSinglePlusConditionCheck processFunction(String function, RuleExecutionContext ruleExecutionContext, boolean isCustom,
                                                            String valueParam, JsonObject condition,
                                                            boolean conditionsMet, String ruleConstVal) {
    if (leader != null && condition.getBoolean("LDR") != null) {

      //the rule also has a condition on the leader field
      //whose value also needs to be passed into any declared function
      ruleExecutionContext.setSubFieldValue(leader.toString());
    }

    ruleExecutionContext.setRuleParameter(condition.getJsonObject("parameter"));
    if (CUSTOM.equals(function.trim())) {
      try {
        if (valueParam == null) {
          throw new NullPointerException("valueParam == null");
        }
        String data = (String) JSManager.runJScript(valueParam, ruleExecutionContext.getSubFieldValue());
        ruleExecutionContext.setSubFieldValue(data);
      } catch (Exception e) {

        //the function has thrown an exception meaning this condition has failed,
        //hence this specific rule has failed
        conditionsMet = false;
        LOGGER.warn(e.getMessage(), e);
      }
    } else {
      String c = NormalizationFunctionRunner.runFunction(function, ruleExecutionContext);
      if (valueParam != null && !c.equals(valueParam) && !isCustom) {

        //still allow a condition to compare the output of a function on the data to a constant value
        //unless this is a custom javascript function in which case, the value holds the custom function
        return new ProcessedSinglePlusConditionCheck(ruleExecutionContext.getSubFieldValue(), true, false);

      } else if (ruleConstVal == null) {

        //if there is no val to use as a replacement , then assume the function
        //is doing generating the needed value and set the data to the returned value
        ruleExecutionContext.setSubFieldValue(c);
      }
    }
    return new ProcessedSinglePlusConditionCheck(ruleExecutionContext.getSubFieldValue(), false, conditionsMet);
  }

  private boolean checkIfAnyFunctionIsCustom(String[] functions, boolean isCustom) {

    //we need to know if one of the functions is a custom function
    //so that we know how to handle the value field - the custom indication
    //may not be the first function listed in the function list
    //a little wasteful, but this will probably only loop at most over 2 or 3 function names
    for (String function : functions) {
      if (CUSTOM.equals(function.trim())) {
        isCustom = true;
        break;
      }
    }
    return isCustom;
  }

  /**
   * create the need part of the instance object based on the target and the string containing the
   * content per subfield sets
   *
   * @param embeddedFields     - the target
   * @param rememberComplexObj - the current object within the instance object we are currently populating
   *                           this can be null if we are now creating a new object within the instance object
   * @return whether a new object was created (boolean)
   */
  private boolean createNewObject(String[] embeddedFields, String data, Object[] rememberComplexObj) {

    if (!data.isEmpty()) {
      Object val = getValue(entity, embeddedFields, data);
      try {
        return buildObject(entity, embeddedFields, createNewComplexObj, val, rememberComplexObj);
      } catch (Exception e) {
        LOGGER.warn(e.getMessage(), e);
        return false;
      }
    }
    return false;
  }

  /**
   * buffers2concat - list of string buffers, each one representing the data belonging to a set of
   * subfields concatenated together, so for example, 2 sets of subfields will mean two entries in the list
   *
   * @return the generated data string
   */
  private String generateDataString() {
    StringBuilder finalData = new StringBuilder();
    for (StringBuilder sb : buffers2concat) {
      if (!sb.isEmpty()) {
        if (!finalData.isEmpty()) {
          finalData.append(separator);
        }
        finalData.append(sb);
      }
    }
    return finalData.toString();
  }

  /**
   * replace the existing subfields in the datafield with subfields generated on the data of the subfield
   * for example: $aitaspa in 041 would be the language of the record. this can be split into two $a subfields
   * $aita and $aspa so that it can be concatenated properly or even become two separate fields with the
   * entity per repeated subfield flag
   * the data is expanded by the implementing function (can be custom as well) - the implementing function
   * receives data from ONE subfield at a time - two $a subfields will be processed separately.
   *
   * @param subFields - sub fields not yet expanded
   * @param splitConf - (add description)
   * @throws ScriptException - (add description)
   */
  private void expandSubfields(List<Subfield> subFields, JsonObject splitConf) throws ScriptException {

    List<Subfield> expandedSubs = new ArrayList<>();
    String func = splitConf.getString(TYPE);
    boolean isCustom = CUSTOM.equals(func);

    String param = splitConf.getString(VALUE);
    for (Subfield subField : subFields) {

      String data = subField.getData();
      Iterator<?> splitData;

      if (isCustom) {
        try {
          splitData = ((Map<?, ?>) JSManager.runJScript(param, data)).values().iterator();
        } catch (Exception e) {
          LOGGER.warn("expandSubfields:: Expanding a field via subFieldSplit must return an array of results. ");
          throw e;
        }
      } else {
        splitData = NormalizationFunctionRunner.runSplitFunction(func, data, param);
      }

      while (splitData.hasNext()) {
        String newData = (String) splitData.next();
        Subfield expandedSub = new SubfieldImpl(subField.getCode(), newData);
        expandedSubs.add(expandedSub);
      }
    }
    subFields.clear();
    subFields.addAll(expandedSubs);
  }

  private static Object getValue(Object object, String[] path, String value) {

    Class<?> type = Integer.TYPE;
    for (String pathSegment : path) {
      try {
        Field field = getField(object.getClass(), pathSegment);
        type = field.getType();
        if (type.isAssignableFrom(List.class) || type.isAssignableFrom(Set.class)) {
          ParameterizedType listType = getParameterizedType(field);
          type = (Class<?>) listType.getActualTypeArguments()[0];
          object = type.getDeclaredConstructor().newInstance();
        }
      } catch (Exception e) {
        LOGGER.warn(e.getMessage(), e);
      }
    }
    return getValue(type, value);
  }

  private static Object getValue(Class<?> type, String value) {

    Object val;
    if (type.isAssignableFrom(String.class)) {
      val = value;
    } else if (type.isAssignableFrom(Boolean.class)) {
      val = Boolean.valueOf(value);
    } else if (type.isAssignableFrom(Double.class)) {
      val = Double.valueOf(value);
    } else {
      val = Integer.valueOf(value);
    }
    return val;
  }

  /**
   * @param object                   - the root object to start parsing the 'path' from
   * @param path                     - the target path - the field to place the value in
   * @param newComp                  - should a new object be created , if not, use the object passed into the
   *                                 complexPreviouslyCreated parameter and continue populating it.
   * @param val                      - target object
   * @param complexPreviouslyCreated - pass in a non primitive pojo that is already partially
   *                                 populated from previous subfield values
   * @return                         - returns boolean based on if new object has been built
   */
  static boolean buildObject(Object object, String[] path, boolean newComp, Object val,
                             Object[] complexPreviouslyCreated) {
    for (String pathSegment : path) {
      try {
        Field field = getField(object.getClass(), pathSegment);
        Class<?> type = field.getType();
        if (type.isAssignableFrom(List.class) || type.isAssignableFrom(Set.class)) {
          // handle collection field
          Method method = getMethod(object.getClass(), columnNametoCamelCaseWithget(pathSegment));
          Collection<Object> coll = setColl(method, object);
          ParameterizedType listType = getParameterizedType(field);
          Class<?> listTypeClass = (Class<?>) listType.getActualTypeArguments()[0];
          if (isPrimitiveOrPrimitiveWrapperOrString(listTypeClass)) {
            coll.add(val);
          } else {
            object = setObjectCorrectly(newComp, listTypeClass, type, pathSegment, coll, object, complexPreviouslyCreated[0]);
            complexPreviouslyCreated[0] = object;
          }
        } else if (!isPrimitiveOrPrimitiveWrapperOrString(type)) {

          //currently not needed for instances, may be needed in the future
          //non primitive member in instance object but represented as a list or set of non
          //primitive objects
          object = getMethod(object.getClass(), columnNametoCamelCaseWithget(pathSegment)).invoke(object);
        } else { // primitive
          getMethod(object.getClass(), columnNametoCamelCaseWithset(pathSegment), val.getClass())
            .invoke(object, val);
        }
      } catch (Exception e) {
        LOGGER.warn(e.getMessage(), e);
        return false;
      }
    }
    return true;
  }

  private static Field getField(Class<?> clazz, String fieldName) {
    return FIELD_CACHE.computeIfAbsent(clazz, k -> new ConcurrentHashMap<>())
      .computeIfAbsent(fieldName, k -> {
        try {
          Field field = LoaderHelper.getField(clazz, fieldName);
          field.setAccessible(true);
          return field;
        } catch (NoSuchFieldException e) {
          LOGGER.error("Couldn't find field: {}", fieldName, e);
          return null;
        }
      });
  }

  private static Method getMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes) {
    return METHOD_CACHE.computeIfAbsent(clazz, k -> new ConcurrentHashMap<>())
      .computeIfAbsent(methodName + Arrays.toString(parameterTypes), k -> {
        try {
          Method method = clazz.getMethod(methodName, parameterTypes);
          method.setAccessible(true);
          return method;
        } catch (NoSuchMethodException e) {
          LOGGER.error("Couldn't find method: {}", methodName, e);
          return null;
        }
      });
  }

  private static ParameterizedType getParameterizedType(Field field) {
    return PARAM_TYPE_CACHE.computeIfAbsent(field, fieldObj -> (ParameterizedType) fieldObj.getGenericType());
  }

  private static Object setObjectCorrectly(boolean newComp, Class<?> listTypeClass, Class<?> type, String pathSegment,
                                           Collection<Object> coll, Object object, Object complexPreviouslyCreated)
    throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {

    if (newComp) {
      Object o = listTypeClass.getDeclaredConstructor().newInstance();
      coll.add(o);
      getMethod(object.getClass(), columnNametoCamelCaseWithset(pathSegment), type).invoke(object, coll);
      return o;
    } else if ((complexPreviouslyCreated != null) &&
      (complexPreviouslyCreated.getClass().isAssignableFrom(listTypeClass))) {
      return complexPreviouslyCreated;
    }
    return object;
  }

  private static Collection<Object> setColl(Method method, Object object) throws InvocationTargetException,
    IllegalAccessException {
    return ((Collection<Object>) method.invoke(object));
  }

  private static String columnNametoCamelCaseWithset(String str) {
    StringBuilder sb = new StringBuilder(str);
    sb.replace(0, 1, String.valueOf(Character.toUpperCase(sb.charAt(0))));
    for (int i = 0; i < sb.length(); i++) {
      if (sb.charAt(i) == '_') {
        sb.deleteCharAt(i);
        sb.replace(i, i + 1, String.valueOf(Character.toUpperCase(sb.charAt(i))));
      }
    }
    return "set" + sb;
  }

  private static String columnNametoCamelCaseWithget(String str) {
    StringBuilder sb = new StringBuilder(str);
    sb.replace(0, 1, String.valueOf(Character.toUpperCase(sb.charAt(0))));
    for (int i = 0; i < sb.length(); i++) {
      if (sb.charAt(i) == '_') {
        sb.deleteCharAt(i);
        sb.replace(i, i + 1, String.valueOf(Character.toUpperCase(sb.charAt(i))));
      }
    }
    return "get" + sb;
  }

  public boolean checkIfSubfieldShouldBeHandled(Set<String> subFieldsSet, Subfield subfield) {
    return subFieldsSet.isEmpty() || subFieldsSet.contains(Character.toString(subfield.getCode()));
  }

  /**
   * Extends regular entity mapping for 1xx, 4xx, 5xx field with "subFieldDelimiter"
   * by adding the following structure to the mapping:
   * "subFieldDelimiter": [
   *             {
   *               "value": " ",
   *               "subfields": [
   *                 "a","b","c","d","t","f","g",...
   *               ]
   *             },
   *             {
   *               "value": "--",
   *               "subfields": [
   *                 "x","y","z","v"
   *               ]
   *             },
   *             {
   *               "value": "--",
   *               "subfields": []
   *             }
   *           ]
   */
  private void addSubFieldDelimiterForAuthorities(DataField dataField, JsonArray mappingArray) {
    if (!dataField.getTag().startsWith("1")
      && !dataField.getTag().startsWith("4")
      && !dataField.getTag().startsWith("5")) {
      return;
    }
    final List<String> doubleDashedSubfields = List.of("x", "y", "z", "v");
    List<LinkedHashMap<String, Object>> mappingList = mappingArray.getList();
    mappingList.forEach(mapping -> {
      List<String> subfields = (List) mapping.get(SUBFIELD);
      if (subfields == null || subfields.stream().noneMatch(doubleDashedSubfields::contains)) {
        return;
      }
      List<LinkedHashMap<String, Object>> subFieldDelimiterList = new ArrayList<>();
      LinkedHashMap<String, Object> spaceDelimiter = new LinkedHashMap<>(Map.of(VALUE, " ", DELIMITER_SUBFIELDS,
        subfields.stream().filter(s -> !doubleDashedSubfields.contains(s)).toList()));
      subFieldDelimiterList.add(spaceDelimiter);
      subFieldDelimiterList.add(new LinkedHashMap<>(Map.of(VALUE, "--", DELIMITER_SUBFIELDS,
        doubleDashedSubfields)));
      subFieldDelimiterList.add(new LinkedHashMap<>(Map.of(VALUE, "--", DELIMITER_SUBFIELDS, List.of())));
      mapping.put("subFieldDelimiter", subFieldDelimiterList);
      }
    );
  }

  /**
   * Extends regular entity mapping for 5xx field with one according to the following rules:
   * additionally to the regular mapping adds the mapping for targets:
   * saftBroaderTerm,  when the control subfield $w has "g" value
   * saftNarrowerTerm, when the control subfield $w has "h" value
   * saftEarlierHeading, when the control subfield $w has "a" value
   * saftLaterHeading, when the control subfield $w has "b" value.
   * saft*Trunc for every saft* field with "i" and numeric subfields excluded
   */
  private JsonArray addExtraMappingsForAuthorities(final DataField dataField, final JsonArray regularMapping) {
    boolean is5XXField = dataField.getTag().startsWith("5");
    if (!is5XXField || regularMapping == null || regularMapping.isEmpty()) {
      return regularMapping;
    }
    final JsonArray extendedMapping = new JsonArray();
    List<String> targets = retrieveTargetsFromControlSubfield(dataField);
    List<LinkedHashMap<String, Object>> mappingList = regularMapping.getList();
    List<LinkedHashMap<String, Object>> truncatedMappingList = createTruncatedMappingList(mappingList);
    targets.forEach(target ->  extendedMapping.addAll(createRelationsMappingForTarget(target, truncatedMappingList)));
    extendedMapping.addAll(regularMapping);
    extendedMapping.addAll(new JsonArray(truncatedMappingList));
    return extendedMapping;
  }

  /**
   * Creates a new mapping list from the original one
   * where subfields "i" and numeric subfields are excluded
   */
  private List<LinkedHashMap<String, Object>> createTruncatedMappingList(
    final List<LinkedHashMap<String, Object>> originalMappingList) {
    List<LinkedHashMap<String, Object>> truncatedMappingList = new ArrayList<>();
    originalMappingList.forEach(map -> {
      LinkedHashMap<String, Object> truncatedMappingMap = new LinkedHashMap<>();
      map.forEach((key, value) -> {
        if (key.equals(TARGET)) {
          truncatedMappingMap.put(TARGET, value + FIELDS_WITH_TRUNCATED_MAPPING_POSTFIX);
          return;
        }
        if (key.equals(SUBFIELD)) {
          List<String> subfieldList = (List) value;
          List<String> truncatedSubfieldList = subfieldList.stream()
            .filter(s -> !s.matches("[0-9i]"))
            .toList();
          truncatedMappingMap.put(SUBFIELD, truncatedSubfieldList);
          return;
        }
        truncatedMappingMap.put(key, value);
      });
      truncatedMappingList.add(truncatedMappingMap);
    });
    return truncatedMappingList;
  }

  private List<String> retrieveTargetsFromControlSubfield(DataField dataField) {
    List<String> targets = new ArrayList<>();
    if (dataField.getSubfield('w') == null) {
      return targets;
    }
    String subfieldData = dataField.getSubfield('w').getData();
    if (subfieldData.contains("g")) {
      targets.add("saftBroaderTerm");
    }
    if (subfieldData.contains("h")) {
      targets.add("saftNarrowerTerm");
    }
    if (subfieldData.contains("a")) {
      targets.add("saftEarlierHeading");
    }
    if (subfieldData.contains("b")) {
      targets.add("saftLaterHeading");
    }
    return targets;
  }

  private void buildAndFillSimpleObject(Object entity, String[] embeddedFields, String value) {
    Object currentObject = entity;
    try {
      currentObject = getOrCreateNestedObject(embeddedFields, currentObject);
      setFieldValueFromPath(embeddedFields, value, currentObject);
    } catch (Exception e) {
      LOGGER.warn("buildSimpleJsonObject:: Error in building simple JsonObject in the mapping process: ", e);
    }
  }

  private void setFieldValueFromPath(String[] embeddedFields, String value, Object currentObject) throws NoSuchFieldException, IllegalAccessException {
    Field targetField = currentObject.getClass().getDeclaredField(embeddedFields[embeddedFields.length - 1]);
    targetField.setAccessible(true);
    targetField.set(currentObject, value);
  }

  private static Object getOrCreateNestedObject(String[] embeddedFields, Object currentObject) throws NoSuchFieldException, IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
    Object nextObject = null;
    for (int i = 0; i < embeddedFields.length - 1; i++) {
      Field field = currentObject.getClass().getDeclaredField(embeddedFields[i]);
      field.setAccessible(true);
      nextObject = field.get(currentObject);
      if (nextObject == null) {
        nextObject = field.getType().getDeclaredConstructor().newInstance();
        field.set(currentObject, nextObject);
      }
    }
    return (nextObject == null) ? currentObject : nextObject;
  }

  /**
   * Constructs the list of rules to map relations like:
   *  {
   *     "entityPerRepeatedSubfield": false,
   *     "entity": [
   *       {
   *         "target": "saftBroaderTerm.headingRef",
   *         "description": "saftMeetingName",
   *         "subfield": ["a","c","d","n","q","g"],
   *         "exclusiveSubfield": ["t"],
   *         "rules": []
   *       },
   *       {
   *         "target": "saftBroaderTerm.headingType",
   *         "description": "meetingName",
   *         "subfield": ["a","c","d","n","q","g"],
   *         "exclusiveSubfield": ["t"],
   *         "rules": [
   *           {
   *             "conditions": [
   *               {
   *                 "type": "set_heading_type_by_name",
   *                 "parameter": {"name": "meetingName"}
   *               }
   *             ]
   *           }
   *         ],
   *         "applyRulesOnConcatenatedData": true
   *       }
   *     ]
   *   }
   */
  private JsonArray createRelationsMappingForTarget(String target, List<LinkedHashMap<String, Object>> existingMappingList) {
    JsonArray additionalMappings = new JsonArray();
    existingMappingList.forEach(existingMap -> {

      Map<String, Object> headingRefMapping = new LinkedHashMap<>(existingMap);
      headingRefMapping.put(TARGET, target + ".headingRef");
      headingRefMapping.put("description", existingMap.get(TARGET));

      Map<String, Object> headingTypeMapping = new LinkedHashMap<>(existingMap);
      headingTypeMapping.put(TARGET, target + ".headingType");
      headingTypeMapping.put("description", getHeadingType(existingMap.get(TARGET)));
      headingTypeMapping.put("applyRulesOnConcatenatedData", true);
      JsonArray entityRules = JsonArray.of(new JsonObject(Map.of("conditions",
        JsonArray.of(JsonObject.of("type", "set_heading_type_by_name",
          "parameter", JsonObject.of("name", getHeadingType(existingMap.get(TARGET))))))));
      headingTypeMapping.put(RULES, entityRules);

      JsonObject additionalMapping = JsonObject.of("entityPerRepeatedSubfield", false,
        "entity", JsonArray.of(headingRefMapping, headingTypeMapping));
      additionalMappings.add(additionalMapping);
    });
    return additionalMappings;
  }

  private static Object getHeadingType(Object headingField) {
    String headingRType = headingField.toString().replace(SAFT_FIELDS_PREFIX, "");
    return Character.toLowerCase(headingRType.charAt(0)) + headingRType.substring(1);
  }
}
