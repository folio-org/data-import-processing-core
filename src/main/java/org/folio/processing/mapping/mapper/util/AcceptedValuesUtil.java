package org.folio.processing.mapping.mapper.util;

import io.vertx.core.json.JsonObject;
import org.folio.Organization;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Map.entry;
import static org.folio.processing.matching.reader.util.MatchIdProcessorUtil.CODE_PROPERTY;
import static org.folio.processing.matching.reader.util.MatchIdProcessorUtil.ID_PROPERTY;
import static org.folio.processing.matching.reader.util.MatchIdProcessorUtil.NAME_PROPERTY;

public class AcceptedValuesUtil {
  private static final String VALUE_PROPERTY = "value";

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
  private static final String VENDOR = "vendor";
  private static final String MATERIAL_SUPPLIER = "materialSupplier";
  private static final String ACCESS_PROVIDER = "accessProvider";
  private static final String DONOR_ORGANIZATION_IDS = "donorOrganizationIds";
  private static final String ORDER_LOCATION = "locationId";
  private static final String ORDER_MATERIAL_TYPE = "materialType";
  private static final String ACQUISITION_UNIT_IDS = "acqUnitIds";
  private static final String BILL_TO = "billTo";
  private static final String SHIP_TO = "shipTo";
  private static final String PRODUCT_ID_TYPE = "productIdType";
  private static final String ACQUISITION_METHOD = "acquisitionMethod";
  private static final String FUND_ID = "fundId";
  private static final String EXPENSE_CLASS_ID = "expenseClassId";

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
    entry(DONOR_ORGANIZATION_IDS, AcceptedValuesUtil::getDonorOrganizationsFromMappingParameters),
    entry(ACQUISITION_UNIT_IDS, MappingParameters::getAcquisitionsUnits),
    entry(BILL_TO, MappingParameters::getTenantConfigurationAddresses),
    entry(SHIP_TO, MappingParameters::getTenantConfigurationAddresses),
    entry(PRODUCT_ID_TYPE, MappingParameters::getIdentifierTypes),
    entry(ACQUISITION_METHOD, MappingParameters::getAcquisitionMethods),
    entry(FUND_ID, MappingParameters::getFunds),
    entry(EXPENSE_CLASS_ID, MappingParameters::getExpenseClasses));

  private AcceptedValuesUtil() {}

  public static Map<String, String> getAcceptedValues(String ruleName, MappingParameters mappingParameters) {
    HashMap<String, String> acceptedValues = new HashMap<>();

    if (!ruleNameToMappingParameter.containsKey(ruleName)) {
      return acceptedValues;
    }

    List<?> mappingParameter = ruleNameToMappingParameter.get(ruleName).apply(mappingParameters);

    mappingParameter.forEach(o -> {
      JsonObject jsonObject = o instanceof String ? new JsonObject((String) o) : JsonObject.mapFrom(o);

      String idField = jsonObject.getString(ID_PROPERTY);
      String nameField = jsonObject.getString(NAME_PROPERTY);
      String valueField = jsonObject.getString(VALUE_PROPERTY);
      String codeField = jsonObject.getString(CODE_PROPERTY);

      if (idField != null && (nameField != null || valueField != null)) {
        StringBuilder value = new StringBuilder()
          .append(nameField != null ? nameField : valueField);

        if (codeField != null) {
          value.append(" (")
            .append(codeField)
            .append(")");
        }
        acceptedValues.put(idField, String.valueOf(value));
      }
    });

    return acceptedValues;
  }

  private static List<Organization> getDonorOrganizationsFromMappingParameters(MappingParameters mappingParameters) {
    return mappingParameters.getOrganizations().stream()
            .filter(organization -> Boolean.TRUE.equals(organization.getIsDonor())).toList();
  }
}
