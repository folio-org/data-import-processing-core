package org.folio.processing.mapping.reader;

import org.folio.AcquisitionMethod;
import org.folio.AcquisitionsUnit;
import org.folio.CallNumberType;
import org.folio.ContributorNameType;
import org.folio.ElectronicAccessRelationship;
import org.folio.ExpenseClass;
import org.folio.Fund;
import org.folio.HoldingsNoteType;
import org.folio.HoldingsType;
import org.folio.IdentifierType;
import org.folio.IllPolicy;
import org.folio.InstanceRelationshipType;
import org.folio.InstanceStatus;
import org.folio.ItemDamageStatus;
import org.folio.ItemNoteType;
import org.folio.Loantype;
import org.folio.Location;
import org.folio.Mtype;
import org.folio.NatureOfContentTerm;
import org.folio.Organization;
import org.folio.StatisticalCode;
import org.folio.StatisticalCodeType;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.util.AcceptedValuesUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class AcceptedValuesUtilTest {
  private static final List<String> INSTANCE_ACCEPTED_VALUES_RULES =
    List.of("statusId", "natureOfContentTermId", "instanceRelationshipTypeId");
  private static final List<String> HOLDINGS_ACCEPTED_VALUES_RULES =
    List.of("holdingsTypeId", "permanentLocationId", "temporaryLocationId", "callNumberTypeId", "illPolicyId", "noteType", "relationshipId");
  private static final List<String> ITEM_ACCEPTED_VALUES_RULES =
    List.of("materialType.id", "itemLevelCallNumberTypeId", "itemDamagedStatusId", "itemNoteTypeId", "permanentLoanType.id",
            "temporaryLoanType.id", "permanentLocation.id", "temporaryLocation.id");
  private static final List<String> ORDER_ACCEPTED_VALUES_RULES =
    List.of("acqUnitIds", "billTo", "shipTo", "contributorNameTypeId", "productIdType",
      "acquisitionMethod", "fundId", "expenseClassId", "locationId", "materialType", "accessProvider", "vendor", "materialSupplier", "donorOrganizationIds");
  private static final String TEST_NAME = "testName";
  public static final String TEST_ADDRESS_TEMPLATE = "{\"id\":\"%s\", \"name\":\"%s\",\"address\":\"Test2\"}";

  @Test
  public void testInstanceAcceptedValues() {
    String testUUID = UUID.randomUUID().toString();

    MappingParameters mappingParameters = new MappingParameters()
      .withInstanceStatuses(List.of(new InstanceStatus().withId(testUUID + "statusId").withName(TEST_NAME)))
      .withNatureOfContentTerms(List.of(new NatureOfContentTerm().withId(testUUID + "natureOfContentTermId").withName(TEST_NAME)))
      .withInstanceRelationshipTypes(List.of(new InstanceRelationshipType().withId(testUUID + "instanceRelationshipTypeId").withName(TEST_NAME)));

    testAcceptedValues(INSTANCE_ACCEPTED_VALUES_RULES, mappingParameters, testUUID);
  }

  @Test
  public void testHoldingsAcceptedValues() {
    String testUUID = UUID.randomUUID().toString();

    MappingParameters mappingParameters = new MappingParameters()
      .withHoldingsTypes(List.of(new HoldingsType().withId(testUUID + "holdingsTypeId").withName(TEST_NAME)))
      .withLocations(List.of(new Location().withId(testUUID + "permanentLocationId").withName(TEST_NAME), new Location().withId(testUUID + "temporaryLocationId").withName(TEST_NAME)))
      .withCallNumberTypes(List.of(new CallNumberType().withId(testUUID + "callNumberTypeId").withName(TEST_NAME)))
      .withIllPolicies(List.of(new IllPolicy().withId(testUUID + "illPolicyId").withName(TEST_NAME)))
      .withHoldingsNoteTypes(List.of(new HoldingsNoteType().withId(testUUID + "noteType").withName(TEST_NAME)))
      .withElectronicAccessRelationships(List.of(new ElectronicAccessRelationship().withId(testUUID + "relationshipId").withName(TEST_NAME)));

    testAcceptedValues(HOLDINGS_ACCEPTED_VALUES_RULES, mappingParameters, testUUID);
  }

  @Test
  public void testItemAcceptedValues() {
    String testUUID = UUID.randomUUID().toString();

    MappingParameters mappingParameters = new MappingParameters()
      .withMaterialTypes(List.of(new Mtype().withId(testUUID + "materialType.id").withName(TEST_NAME)))
      .withCallNumberTypes(List.of(new CallNumberType().withId(testUUID + "itemLevelCallNumberTypeId").withName(TEST_NAME)))
      .withItemDamagedStatuses(List.of(new ItemDamageStatus().withId(testUUID + "itemDamagedStatusId").withName(TEST_NAME)))
      .withItemNoteTypes(List.of(new ItemNoteType().withId(testUUID + "itemNoteTypeId").withName(TEST_NAME)))
      .withLoanTypes(List.of(new Loantype().withId(testUUID + "permanentLoanType.id").withName(TEST_NAME), new Loantype().withId(testUUID + "temporaryLoanType.id").withName(TEST_NAME)))
      .withLocations(List.of(new Location().withId(testUUID + "permanentLocation.id").withName(TEST_NAME), new Location().withId(testUUID + "temporaryLocation.id").withName(TEST_NAME)));

    testAcceptedValues(ITEM_ACCEPTED_VALUES_RULES, mappingParameters, testUUID);
  }

  @Test
  public void testOrderAcceptedValues() {
    String testUUID = UUID.randomUUID().toString();

    MappingParameters mappingParameters = new MappingParameters()
      .withAcquisitionsUnits(List.of(new AcquisitionsUnit().withId(testUUID + "acqUnitIds").withName(TEST_NAME)))
      .withTenantConfigurationAddresses(List.of(String.format(TEST_ADDRESS_TEMPLATE, testUUID + "billTo", TEST_NAME),
        String.format(TEST_ADDRESS_TEMPLATE, testUUID + "shipTo", TEST_NAME)))
      .withContributorNameTypes(List.of(new ContributorNameType().withId(testUUID + "contributorNameTypeId").withName(TEST_NAME)))
      .withIdentifierTypes(List.of(new IdentifierType().withId(testUUID + "productIdType").withName(TEST_NAME)))
      .withAcquisitionMethods(List.of(new AcquisitionMethod().withId(testUUID + "acquisitionMethod").withValue(TEST_NAME)))
      .withFunds(List.of(new Fund().withId(testUUID + "fundId").withName(TEST_NAME)))
      .withExpenseClasses(List.of(new ExpenseClass().withId(testUUID + "expenseClassId").withName(TEST_NAME)))
      .withLocations(List.of(new Location().withId(testUUID + "locationId").withName(TEST_NAME)))
      .withMaterialTypes(List.of(new Mtype().withId(testUUID + "materialType").withName(TEST_NAME)))
      .withOrganizations(List.of(new Organization().withId(testUUID + "accessProvider").withName(TEST_NAME),
        new Organization().withId(testUUID + "vendor").withName(TEST_NAME),
        new Organization().withId(testUUID + "donorOrganizationIds").withIsDonor(true).withName(TEST_NAME),
        new Organization().withId(testUUID + "materialSupplier").withName(TEST_NAME)));

    testAcceptedValues(ORDER_ACCEPTED_VALUES_RULES, mappingParameters, testUUID);
  }

  @Test
  public void shouldReturnEmptyAcceptedValuesIfIdIsNull() {
    Map<String, String> map = AcceptedValuesUtil.getAcceptedValues("billTo",
      new MappingParameters().withTenantConfigurationAddresses(List.of("{\"name\":\"test\",\"address\":\"Test2\"}")));

    assertTrue(map.isEmpty());
  }

  @Test
  public void shouldReturnEmptyAcceptedValuesIfNameIsNull() {
    Map<String, String> map = AcceptedValuesUtil.getAcceptedValues("billTo",
      new MappingParameters().withTenantConfigurationAddresses(List.of("{\"id\":\"test\",\"address\":\"Test2\"}")));

    assertTrue(map.isEmpty());
  }

  @Test
  public void testStatisticalCodeFormation() {
    String statCodeUUID = UUID.randomUUID().toString();
    String statCodeTypeUUID = UUID.randomUUID().toString();

    MappingParameters mappingParameters = new MappingParameters()
      .withStatisticalCodes(List.of(new StatisticalCode().withId(statCodeUUID).withName("Test Code").withCode("test").withStatisticalCodeTypeId(statCodeTypeUUID)))
      .withStatisticalCodeTypes(List.of(new StatisticalCodeType().withId(statCodeTypeUUID).withName("TEST (testing)")));

    Map<String, String> acceptedValues = AcceptedValuesUtil.getAcceptedValues("statisticalCodeId", mappingParameters);

    assertFalse(acceptedValues.isEmpty());
    assertTrue(acceptedValues.containsKey(statCodeUUID));
    assertEquals("TEST (testing): test - Test Code", acceptedValues.get(statCodeUUID));
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
