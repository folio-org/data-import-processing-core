package org.folio.processing.mapping.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.folio.AcquisitionMethod;
import org.folio.AcquisitionsUnit;
import org.folio.ExpenseClass;
import org.folio.Fund;
import org.folio.Organization;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.util.AcceptedValuesUtil;
import org.folio.rest.jaxrs.model.CallNumberType;
import org.folio.rest.jaxrs.model.ContributorNameType;
import org.folio.rest.jaxrs.model.ElectronicAccessRelationship;
import org.folio.rest.jaxrs.model.HoldingsNoteType;
import org.folio.rest.jaxrs.model.HoldingsType;
import org.folio.rest.jaxrs.model.IdentifierType;
import org.folio.rest.jaxrs.model.IllPolicy;
import org.folio.rest.jaxrs.model.InstanceRelationshipType;
import org.folio.rest.jaxrs.model.InstanceStatus;
import org.folio.rest.jaxrs.model.ItemDamageStatus;
import org.folio.rest.jaxrs.model.ItemNoteType;
import org.folio.rest.jaxrs.model.LoanType;
import org.folio.rest.jaxrs.model.Location;
import org.folio.rest.jaxrs.model.MaterialType;
import org.folio.rest.jaxrs.model.NatureOfContentTerm;
import org.folio.rest.jaxrs.model.StatisticalCode;
import org.folio.rest.jaxrs.model.StatisticalCodeType;
import org.junit.jupiter.api.Test;

class AcceptedValuesUtilTest {
  private static final List<String> INSTANCE_ACCEPTED_VALUES_RULES =
    List.of("statusId", "natureOfContentTermId", "instanceRelationshipTypeId");
  private static final List<String> HOLDINGS_ACCEPTED_VALUES_RULES =
    List.of("holdingsTypeId", "permanentLocationId", "temporaryLocationId", "callNumberTypeId", "illPolicyId",
      "noteType", "relationshipId");
  private static final List<String> ITEM_ACCEPTED_VALUES_RULES =
    List.of("materialType.id", "itemLevelCallNumberTypeId", "itemDamagedStatusId", "itemNoteTypeId",
      "permanentLoanType.id",
      "temporaryLoanType.id", "permanentLocation.id", "temporaryLocation.id");
  private static final List<String> ORDER_ACCEPTED_VALUES_RULES =
    List.of("acqUnitIds", "billTo", "shipTo", "contributorNameTypeId", "productIdType",
      "acquisitionMethod", "fundId", "expenseClassId", "locationId", "materialType", "accessProvider", "vendor",
      "materialSupplier", "donorOrganizationIds");
  private static final String TEST_NAME = "testName";
  private static final String TEST_ADDRESS_TEMPLATE = "{\"id\":\"%s\", \"name\":\"%s\",\"address\":\"Test2\"}";

  @Test
  void testInstanceAcceptedValues() {
    String testUuid = UUID.randomUUID().toString();

    MappingParameters mappingParameters = new MappingParameters()
      .withInstanceStatuses(List.of(new InstanceStatus().withId(testUuid + "statusId").withName(TEST_NAME)))
      .withNatureOfContentTerms(
        List.of(new NatureOfContentTerm().withId(testUuid + "natureOfContentTermId").withName(TEST_NAME)))
      .withInstanceRelationshipTypes(
        List.of(new InstanceRelationshipType().withId(testUuid + "instanceRelationshipTypeId").withName(TEST_NAME)));

    testAcceptedValues(INSTANCE_ACCEPTED_VALUES_RULES, mappingParameters, testUuid);
  }

  @Test
  void testHoldingsAcceptedValues() {
    String testUuid = UUID.randomUUID().toString();

    MappingParameters mappingParameters = new MappingParameters()
      .withHoldingsTypes(List.of(new HoldingsType().withId(testUuid + "holdingsTypeId").withName(TEST_NAME)))
      .withLocations(List.of(new Location().withId(testUuid + "permanentLocationId").withName(TEST_NAME),
        new Location().withId(testUuid + "temporaryLocationId").withName(TEST_NAME)))
      .withCallNumberTypes(List.of(new CallNumberType().withId(testUuid + "callNumberTypeId").withName(TEST_NAME)))
      .withIllPolicies(List.of(new IllPolicy().withId(testUuid + "illPolicyId").withName(TEST_NAME)))
      .withHoldingsNoteTypes(List.of(new HoldingsNoteType().withId(testUuid + "noteType").withName(TEST_NAME)))
      .withElectronicAccessRelationships(
        List.of(new ElectronicAccessRelationship().withId(testUuid + "relationshipId").withName(TEST_NAME)));

    testAcceptedValues(HOLDINGS_ACCEPTED_VALUES_RULES, mappingParameters, testUuid);
  }

  @Test
  void testItemAcceptedValues() {
    String testUuid = UUID.randomUUID().toString();

    MappingParameters mappingParameters = new MappingParameters()
      .withMaterialTypes(List.of(new MaterialType().withId(testUuid + "materialType.id").withName(TEST_NAME)))
      .withCallNumberTypes(
        List.of(new CallNumberType().withId(testUuid + "itemLevelCallNumberTypeId").withName(TEST_NAME)))
      .withItemDamagedStatuses(
        List.of(new ItemDamageStatus().withId(testUuid + "itemDamagedStatusId").withName(TEST_NAME)))
      .withItemNoteTypes(List.of(new ItemNoteType().withId(testUuid + "itemNoteTypeId").withName(TEST_NAME)))
      .withLoanTypes(List.of(new LoanType().withId(testUuid + "permanentLoanType.id").withName(TEST_NAME),
        new LoanType().withId(testUuid + "temporaryLoanType.id").withName(TEST_NAME)))
      .withLocations(List.of(new Location().withId(testUuid + "permanentLocation.id").withName(TEST_NAME),
        new Location().withId(testUuid + "temporaryLocation.id").withName(TEST_NAME)));

    testAcceptedValues(ITEM_ACCEPTED_VALUES_RULES, mappingParameters, testUuid);
  }

  @Test
  void testOrderAcceptedValues() {
    String testUuid = UUID.randomUUID().toString();

    MappingParameters mappingParameters = new MappingParameters()
      .withAcquisitionsUnits(List.of(new AcquisitionsUnit().withId(testUuid + "acqUnitIds").withName(TEST_NAME)))
      .withTenantConfigurationAddresses(List.of(String.format(TEST_ADDRESS_TEMPLATE, testUuid + "billTo", TEST_NAME),
        String.format(TEST_ADDRESS_TEMPLATE, testUuid + "shipTo", TEST_NAME)))
      .withContributorNameTypes(
        List.of(new ContributorNameType().withId(testUuid + "contributorNameTypeId").withName(TEST_NAME)))
      .withIdentifierTypes(List.of(new IdentifierType().withId(testUuid + "productIdType").withName(TEST_NAME)))
      .withAcquisitionMethods(
        List.of(new AcquisitionMethod().withId(testUuid + "acquisitionMethod").withValue(TEST_NAME)))
      .withFunds(List.of(new Fund().withId(testUuid + "fundId").withName(TEST_NAME)))
      .withExpenseClasses(List.of(new ExpenseClass().withId(testUuid + "expenseClassId").withName(TEST_NAME)))
      .withLocations(List.of(new Location().withId(testUuid + "locationId").withName(TEST_NAME)))
      .withMaterialTypes(List.of(new MaterialType().withId(testUuid + "materialType").withName(TEST_NAME)))
      .withOrganizations(List.of(new Organization().withId(testUuid + "accessProvider").withName(TEST_NAME),
        new Organization().withId(testUuid + "vendor").withName(TEST_NAME),
        new Organization().withId(testUuid + "donorOrganizationIds").withIsDonor(true).withName(TEST_NAME),
        new Organization().withId(testUuid + "materialSupplier").withName(TEST_NAME)));

    testAcceptedValues(ORDER_ACCEPTED_VALUES_RULES, mappingParameters, testUuid);
  }

  @Test
  void shouldReturnEmptyAcceptedValuesIfIdIsNull() {
    Map<String, String> map = AcceptedValuesUtil.getAcceptedValues("billTo",
      new MappingParameters().withTenantConfigurationAddresses(List.of("{\"name\":\"test\",\"address\":\"Test2\"}")));

    assertTrue(map.isEmpty());
  }

  @Test
  void shouldReturnEmptyAcceptedValuesIfNameIsNull() {
    Map<String, String> map = AcceptedValuesUtil.getAcceptedValues("billTo",
      new MappingParameters().withTenantConfigurationAddresses(List.of("{\"id\":\"test\",\"address\":\"Test2\"}")));

    assertTrue(map.isEmpty());
  }

  @Test
  void shouldReturnEmptyAcceptedValuesIfRuleNameIsNull() {
    Map<String, String> map = AcceptedValuesUtil.getAcceptedValues(null, new MappingParameters());

    assertTrue(map.isEmpty());
  }

  @Test
  void testStatisticalCodeFormation() {
    String statCodeUuid = UUID.randomUUID().toString();
    String statCodeTypeUuid = UUID.randomUUID().toString();

    MappingParameters mappingParameters = new MappingParameters()
      .withStatisticalCodes(List.of(new StatisticalCode().withId(statCodeUuid).withName("Test Code").withCode("test")
        .withStatisticalCodeTypeId(statCodeTypeUuid)))
      .withStatisticalCodeTypes(List.of(new StatisticalCodeType().withId(statCodeTypeUuid).withName("TEST (testing)")));

    Map<String, String> acceptedValues = AcceptedValuesUtil.getAcceptedValues("statisticalCodeId", mappingParameters);

    assertFalse(acceptedValues.isEmpty());
    assertTrue(acceptedValues.containsKey(statCodeUuid));
    assertEquals("TEST (testing): test - Test Code", acceptedValues.get(statCodeUuid));
  }

  private void testAcceptedValues(List<String> acceptedValuesRules, MappingParameters mappingParameters, String uuid) {
    acceptedValuesRules.forEach(rule -> {
      Map<String, String> acceptedValues = AcceptedValuesUtil.getAcceptedValues(rule, mappingParameters);
      String key = uuid + rule;
      assertFalse(acceptedValues.isEmpty());
      assertTrue(acceptedValues.containsKey(key));
      assertEquals(TEST_NAME, acceptedValues.get(key));
    });
  }
}
