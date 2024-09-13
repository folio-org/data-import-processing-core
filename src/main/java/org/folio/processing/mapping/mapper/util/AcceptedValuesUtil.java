package org.folio.processing.mapping.mapper.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.Organization;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Map.entry;

public class AcceptedValuesUtil {
  private static final Logger LOGGER = LogManager.getLogger(AcceptedValuesUtil.class);

  private static final String HOLDINGS_PERMANENT_LOCATION_ID = "permanentLocationId";
  private static final String HOLDINGS_TEMPORARY_LOCATION_ID = "temporaryLocationId";
  private static final String STATUS_ID = "statusId";
  private static final String NATURE_OF_CONTENT_TERM_ID = "natureOfContentTermId";
  private static final String INSTANCE_RELATIONSHIP_TYPE_ID = "instanceRelationshipTypeId";
  private static final String HOLDINGS_TYPE_ID = "holdingsTypeId";
  private static final String CALL_NUMBER_TYPE_ID = "callNumberTypeId";
  private static final String ILL_POLICY_ID = "illPolicyId";
  private static final String STATISTICAL_CODE_ID = "statisticalCodeId";
  private static final String NOTE_TYPE = "noteType";
  private static final String RELATIONSHIP_ID = "relationshipId";
  private static final String MATERIAL_TYPE_ID = "materialType.id";
  private static final String ITEM_CALL_NUMBER_TYPE_ID = "itemLevelCallNumberTypeId";
  private static final String ITEM_NOTE_TYPE_ID = "itemNoteTypeId";
  private static final String PERMANENT_LOAN_TYPE_ID = "permanentLoanType.id";
  private static final String TEMPORARY_LOAN_TYPE_ID = "temporaryLoanType.id";
  private static final String ITEM_PERMANENT_LOCATION_ID = "permanentLocation.id";
  private static final String ITEM_TEMPORARY_LOCATION_ID = "temporaryLocation.id";
  private static final String ITEM_DAMAGED_STATUS_ID = "itemDamagedStatusId";
  private static final String CONTRIBUTOR_NAME_TYPE_ID = "contributorNameTypeId";
  private static final String ORDER_LOCATION = "locationId";
  private static final String ORDER_MATERIAL_TYPE = "materialType";
  private static final String VENDOR = "vendor";
  private static final String MATERIAL_SUPPLIER = "materialSupplier";
  private static final String ACCESS_PROVIDER = "accessProvider";
  private static final String DONOR_ORGANIZATION_IDS = "donorOrganizationIds";

  private static final Map<String, Function<MappingParameters, List<?>>> ruleNameToMappingParameter = Map.ofEntries(
    entry(HOLDINGS_PERMANENT_LOCATION_ID, MappingParameters::getLocations),
    entry(HOLDINGS_TEMPORARY_LOCATION_ID, MappingParameters::getLocations),
    entry(STATUS_ID, MappingParameters::getInstanceStatuses),
    entry(NATURE_OF_CONTENT_TERM_ID, MappingParameters::getNatureOfContentTerms),
    entry(INSTANCE_RELATIONSHIP_TYPE_ID, MappingParameters::getInstanceRelationshipTypes),
    entry(HOLDINGS_TYPE_ID, MappingParameters::getHoldingsTypes),
    entry(CALL_NUMBER_TYPE_ID, MappingParameters::getCallNumberTypes),
    entry(ILL_POLICY_ID, MappingParameters::getIllPolicies),
    entry(STATISTICAL_CODE_ID, MappingParameters::getStatisticalCodes),
    entry(NOTE_TYPE, MappingParameters::getHoldingsNoteTypes),
    entry(RELATIONSHIP_ID, MappingParameters::getElectronicAccessRelationships),
    entry(MATERIAL_TYPE_ID, MappingParameters::getMaterialTypes),
    entry(ITEM_CALL_NUMBER_TYPE_ID, MappingParameters::getCallNumberTypes),
    entry(ITEM_DAMAGED_STATUS_ID, MappingParameters::getItemDamageStatuses),
    entry(ITEM_NOTE_TYPE_ID, MappingParameters::getItemNoteTypes),
    entry(PERMANENT_LOAN_TYPE_ID, MappingParameters::getLoanTypes),
    entry(TEMPORARY_LOAN_TYPE_ID, MappingParameters::getLoanTypes),
    entry(ITEM_PERMANENT_LOCATION_ID, MappingParameters::getLocations),
    entry(ITEM_TEMPORARY_LOCATION_ID, MappingParameters::getLocations),
    entry(CONTRIBUTOR_NAME_TYPE_ID, MappingParameters::getContributorNameTypes),
    entry(ORDER_LOCATION, MappingParameters::getLocations),
    entry(ORDER_MATERIAL_TYPE, MappingParameters::getMaterialTypes),
    entry(VENDOR, MappingParameters::getOrganizations),
    entry(MATERIAL_SUPPLIER, MappingParameters::getOrganizations),
    entry(ACCESS_PROVIDER, MappingParameters::getOrganizations),
    entry(DONOR_ORGANIZATION_IDS, AcceptedValuesUtil::getDonorOrganizationsFromMappingParameters));

  public static HashMap<String, String> getAcceptedValues(String ruleName, MappingParameters mappingParameters) {
    HashMap<String, String> acceptedValues = new HashMap<>();

    if (!ruleNameToMappingParameter.containsKey(ruleName)) {
      return acceptedValues;
    }

    List<?> mappingParameter = ruleNameToMappingParameter.get(ruleName).apply(mappingParameters);

    mappingParameter.forEach(o -> {
      Class<?> paramClass = o.getClass();

      Optional<Object> id = invokeClassMethod(paramClass, "getId", null, o, null);
      Optional<Object> name = invokeClassMethod(paramClass, "getName", null, o, null);
      Optional<Object> code = invokeClassMethod(paramClass, "getCode", null, o, null);

      if (id.isPresent() && name.isPresent()) {
        StringBuilder value = new StringBuilder()
          .append(name.get());

        code.ifPresent(object ->
          value.append(" (")
            .append(object)
            .append(")"));
        acceptedValues.put(id.get().toString(), String.valueOf(value));
      }
    });

    return acceptedValues;
  }

  private static Optional<Object> invokeClassMethod(Class<?> objectClass, String methodName, Object[] args, Object o, Class<?>[] parameterTypes) {
    try {
      if (isClassContainsMethod(methodName, objectClass)) {
        return Optional.ofNullable(objectClass.getMethod(methodName, parameterTypes).invoke(o, args));
      }
      return Optional.empty();
    } catch (Exception e) {
      String errorMessage = String.format("Error during invoke of class method, class: %s, method: %s", objectClass, methodName);
      LOGGER.warn("invokeClassMethod:: " + errorMessage, e);
      throw new IllegalArgumentException(errorMessage, e);
    }
  }

  private static boolean isClassContainsMethod(String methodName, Class<?> objectClass) {
    return Arrays.stream(objectClass.getMethods()).anyMatch(method -> methodName.equals(method.getName()));
  }

  private static List<Organization> getDonorOrganizationsFromMappingParameters(MappingParameters mappingParameters) {
    return mappingParameters.getOrganizations().stream()
            .filter(organization -> Boolean.TRUE.equals(organization.getIsDonor())).toList();
  }
}
