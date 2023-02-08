package org.folio.processing.mapping.defaultmapper.processor.parameters;

import org.apache.commons.collections4.list.UnmodifiableList;
import org.folio.AlternativeTitleType;
import org.folio.AuthorityNoteType;
import org.folio.AuthoritySourceFile;
import org.folio.CallNumberType;
import org.folio.ClassificationType;
import org.folio.ContributorNameType;
import org.folio.ContributorType;
import org.folio.ElectronicAccessRelationship;
import org.folio.HoldingsNoteType;
import org.folio.HoldingsType;
import org.folio.IdentifierType;
import org.folio.IllPolicy;
import org.folio.InstanceFormat;
import org.folio.InstanceNoteType;
import org.folio.InstanceRelationshipType;
import org.folio.InstanceStatus;
import org.folio.InstanceType;
import org.folio.IssuanceMode;
import org.folio.ItemDamageStatus;
import org.folio.ItemNoteType;
import org.folio.Loantype;
import org.folio.Location;
import org.folio.Mtype;
import org.folio.NatureOfContentTerm;
import org.folio.StatisticalCode;
import org.folio.StatisticalCodeType;
import org.folio.Organization;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Class to store parameters needed for mapping functions
 */
public class MappingParameters {

  private boolean initialized = false;
  private List<IdentifierType> identifierTypes = new ArrayList<>();
  private List<ClassificationType> classificationTypes = new ArrayList<>();
  private List<InstanceType> instanceTypes = new ArrayList<>();
  private List<ElectronicAccessRelationship> electronicAccessRelationships = new ArrayList<>();
  private List<InstanceFormat> instanceFormats = new ArrayList<>();
  private List<ContributorType> contributorTypes = new ArrayList<>();
  private List<ContributorNameType> contributorNameTypes = new ArrayList<>();
  private List<InstanceNoteType> instanceNoteTypes = new ArrayList<>();
  private List<AlternativeTitleType> alternativeTitleTypes = new ArrayList<>();
  private List<IssuanceMode> issuanceModes = new ArrayList<>();
  private List<InstanceStatus> instanceStatuses = new ArrayList<>();
  private List<NatureOfContentTerm> natureOfContentTerms = new ArrayList<>();
  private List<InstanceRelationshipType> instanceRelationshipTypes = new ArrayList<>();
  private List<HoldingsType> holdingsTypes = new ArrayList<>();
  private List<HoldingsNoteType> holdingsNoteTypes = new ArrayList<>();
  private List<IllPolicy> illPolicies = new ArrayList<>();
  private List<CallNumberType> callNumberTypes = new ArrayList<>();
  private List<StatisticalCode> statisticalCodes = new ArrayList<>();
  private List<StatisticalCodeType> statisticalCodeTypes = new ArrayList<>();
  private List<Location> locations = new ArrayList<>();
  private List<Mtype> materialTypes = new ArrayList<>();
  private List<ItemDamageStatus> itemDamageStatuses = new ArrayList<>();
  private List<Loantype> loanTypes = new ArrayList<>();
  private List<ItemNoteType> itemNoteTypes = new ArrayList<>();
  private List<MarcFieldProtectionSetting> marcFieldProtectionSettings = new ArrayList<>();
  private String tenantConfiguration;
  private List<AuthorityNoteType> authorityNoteTypes;
  private List<AuthoritySourceFile> authoritySourceFiles;
  private List<Organization> organizations;

  public MappingParameters withInitializedState(boolean initialized) {
    this.initialized = initialized;
    return this;
  }

  public boolean isInitialized() {
    return initialized;
  }

  public List<IdentifierType> getIdentifierTypes() {
    return identifierTypes;
  }

  public MappingParameters withIdentifierTypes(List<IdentifierType> identifierTypes) {
    this.identifierTypes = new UnmodifiableList<>(identifierTypes);
    return this;
  }

  public List<ClassificationType> getClassificationTypes() {
    return classificationTypes;
  }

  public MappingParameters withClassificationTypes(List<ClassificationType> classificationTypes) {
    this.classificationTypes = new UnmodifiableList<>(classificationTypes);
    return this;
  }

  public List<InstanceType> getInstanceTypes() {
    return instanceTypes;
  }

  public MappingParameters withInstanceTypes(List<InstanceType> instanceTypes) {
    this.instanceTypes = new UnmodifiableList<>(instanceTypes);
    return this;
  }

  public MappingParameters withElectronicAccessRelationships(List<ElectronicAccessRelationship> electronicAccessRelationships) {
    this.electronicAccessRelationships = new UnmodifiableList<>(electronicAccessRelationships);
    return this;
  }

  public List<InstanceFormat> getInstanceFormats() {
    return instanceFormats;
  }

  public MappingParameters withInstanceFormats(List<InstanceFormat> instanceFormats) {
    this.instanceFormats = new UnmodifiableList<>(instanceFormats);
    return this;
  }

  public List<ContributorType> getContributorTypes() {
    return contributorTypes;
  }

  public MappingParameters withContributorTypes(List<ContributorType> contributorTypes) {
    this.contributorTypes = new UnmodifiableList<>(contributorTypes);
    return this;
  }

  public List<ContributorNameType> getContributorNameTypes() {
    return contributorNameTypes;
  }

  public MappingParameters withContributorNameTypes(List<ContributorNameType> contributorNameTypes) {
    this.contributorNameTypes = new UnmodifiableList<>(contributorNameTypes);
    return this;
  }

  public List<InstanceNoteType> getInstanceNoteTypes() {
    return instanceNoteTypes;
  }

  public MappingParameters withInstanceNoteTypes(List<InstanceNoteType> instanceNoteTypes) {
    this.instanceNoteTypes = new UnmodifiableList<>(instanceNoteTypes);
    return this;
  }

  public List<AlternativeTitleType> getAlternativeTitleTypes() {
    return alternativeTitleTypes;
  }

  public MappingParameters withAlternativeTitleTypes(List<AlternativeTitleType> alternativeTitleTypes) {
    this.alternativeTitleTypes = new UnmodifiableList<>(alternativeTitleTypes);
    return this;
  }

  public List<IssuanceMode> getIssuanceModes() {
    return issuanceModes;
  }

  public MappingParameters withIssuanceModes(List<IssuanceMode> issuanceModes) {
    this.issuanceModes = new UnmodifiableList<>(issuanceModes);
    return this;
  }

  public void setInitialized(boolean initialized) {
    this.initialized = initialized;
  }

  public void setIdentifierTypes(List<IdentifierType> identifierTypes) {
    this.identifierTypes = identifierTypes;
  }

  public void setClassificationTypes(List<ClassificationType> classificationTypes) {
    this.classificationTypes = classificationTypes;
  }

  public void setInstanceTypes(List<InstanceType> instanceTypes) {
    this.instanceTypes = instanceTypes;
  }

  public List<ElectronicAccessRelationship> getElectronicAccessRelationships() {
    return electronicAccessRelationships;
  }

  public void setElectronicAccessRelationships(List<ElectronicAccessRelationship> electronicAccessRelationships) {
    this.electronicAccessRelationships = electronicAccessRelationships;
  }

  public void setInstanceFormats(List<InstanceFormat> instanceFormats) {
    this.instanceFormats = instanceFormats;
  }

  public void setContributorTypes(List<ContributorType> contributorTypes) {
    this.contributorTypes = contributorTypes;
  }

  public void setContributorNameTypes(List<ContributorNameType> contributorNameTypes) {
    this.contributorNameTypes = contributorNameTypes;
  }

  public void setInstanceNoteTypes(List<InstanceNoteType> instanceNoteTypes) {
    this.instanceNoteTypes = instanceNoteTypes;
  }

  public void setAlternativeTitleTypes(List<AlternativeTitleType> alternativeTitleTypes) {
    this.alternativeTitleTypes = alternativeTitleTypes;
  }

  public void setIssuanceModes(List<IssuanceMode> issuanceModes) {
    this.issuanceModes = issuanceModes;
  }

  public List<InstanceStatus> getInstanceStatuses() {
    return instanceStatuses;
  }

  public void setInstanceStatuses(List<InstanceStatus> instanceStatuses) {
    this.instanceStatuses = instanceStatuses;
  }

  public MappingParameters withInstanceStatuses(List<InstanceStatus> instanceStatuses) {
    this.instanceStatuses = new UnmodifiableList<>(instanceStatuses);
    return this;
  }

  public MappingParameters withNatureOfContentTerms(List<NatureOfContentTerm> natureOfContentTerms) {
    this.natureOfContentTerms = new UnmodifiableList<>(natureOfContentTerms);
    return this;
  }

  public List<NatureOfContentTerm> getNatureOfContentTerms() {
    return natureOfContentTerms;
  }

  public void setNatureOfContentTerms(List<NatureOfContentTerm> natureOfContentTerms) {
    this.natureOfContentTerms = natureOfContentTerms;
  }

  public List<InstanceRelationshipType> getInstanceRelationshipTypes() {
    return instanceRelationshipTypes;
  }

  public void setInstanceRelationshipTypes(List<InstanceRelationshipType> instanceRelationshipTypes) {
    this.instanceRelationshipTypes = instanceRelationshipTypes;
  }

  public List<HoldingsType> getHoldingsTypes() {
    return holdingsTypes;
  }

  public void setHoldingsTypes(List<HoldingsType> holdingsTypes) {
    this.holdingsTypes = holdingsTypes;
  }

  public List<HoldingsNoteType> getHoldingsNoteTypes() {
    return holdingsNoteTypes;
  }

  public void setHoldingsNoteTypes(List<HoldingsNoteType> holdingsNoteTypes) {
    this.holdingsNoteTypes = holdingsNoteTypes;
  }

  public List<IllPolicy> getIllPolicies() {
    return illPolicies;
  }

  public void setIllPolicies(List<IllPolicy> illPolicies) {
    this.illPolicies = illPolicies;
  }

  public List<CallNumberType> getCallNumberTypes() {
    return callNumberTypes;
  }

  public void setCallNumberTypes(List<CallNumberType> callNumberTypes) {
    this.callNumberTypes = callNumberTypes;
  }

  public List<StatisticalCode> getStatisticalCodes() {
    return statisticalCodes;
  }

  public void setStatisticalCodes(List<StatisticalCode> statisticalCodes) {
    this.statisticalCodes = statisticalCodes;
  }

  public List<StatisticalCodeType> getStatisticalCodeTypes() {
    return statisticalCodeTypes;
  }

  public void setStatisticalCodeTypes(List<StatisticalCodeType> statisticalCodeTypes) {
    this.statisticalCodeTypes = statisticalCodeTypes;
  }

  public List<Location> getLocations() {
    return locations;
  }

  public void setLocations(List<Location> locations) {
    this.locations = locations;
  }

  public List<Mtype> getMaterialTypes() {
    return materialTypes;
  }

  public void setMaterialTypes(List<Mtype> materialTypes) {
    this.materialTypes = materialTypes;
  }

  public List<ItemDamageStatus> getItemDamageStatuses() {
    return itemDamageStatuses;
  }

  public void setItemDamageStatuses(List<ItemDamageStatus> itemDamageStatuses) {
    this.itemDamageStatuses = itemDamageStatuses;
  }

  public List<Loantype> getLoanTypes() {
    return loanTypes;
  }

  public void setLoanTypes(List<Loantype> loanTypes) {
    this.loanTypes = loanTypes;
  }

  public List<ItemNoteType> getItemNoteTypes() {
    return itemNoteTypes;
  }

  public void setItemNoteTypes(List<ItemNoteType> itemNoteTypes) {
    this.itemNoteTypes = itemNoteTypes;
  }

  public List<MarcFieldProtectionSetting> getMarcFieldProtectionSettings() {
    return marcFieldProtectionSettings;
  }

  public String getTenantConfiguration() {
    return tenantConfiguration;
  }

  public void setTenantConfiguration(String tenantConfiguration) {
    this.tenantConfiguration = tenantConfiguration;
  }

  public void setMarcFieldProtectionSettings(List<MarcFieldProtectionSetting> marcFieldProtectionSettings) {
    this.marcFieldProtectionSettings = marcFieldProtectionSettings;
  }

  public List<AuthorityNoteType> getAuthorityNoteTypes() {
    return authorityNoteTypes;
  }

  public void setAuthorityNoteTypes(List<AuthorityNoteType> authorityNoteTypes) {
    this.authorityNoteTypes = authorityNoteTypes;
  }

  public List<AuthoritySourceFile> getAuthoritySourceFiles() {
    return authoritySourceFiles;
  }

  public void setAuthoritySourceFiles(List<AuthoritySourceFile> authoritySourceFiles) {
    this.authoritySourceFiles = authoritySourceFiles;
  }

  public MappingParameters withInstanceRelationshipTypes(List<InstanceRelationshipType> instanceRelationshipTypes) {
    this.instanceRelationshipTypes = new UnmodifiableList<>(instanceRelationshipTypes);
    return this;
  }

  public MappingParameters withHoldingsTypes(List<HoldingsType> holdingsTypes) {
    this.holdingsTypes = new UnmodifiableList<>(holdingsTypes);
    return this;
  }

  public MappingParameters withHoldingsNoteTypes(List<HoldingsNoteType> holdingsNoteTypes) {
    this.holdingsNoteTypes = new UnmodifiableList<>(holdingsNoteTypes);
    return this;
  }

  public MappingParameters withIllPolicies(List<IllPolicy> illPolicies) {
    this.illPolicies = new UnmodifiableList<>(illPolicies);
    return this;
  }

  public MappingParameters withCallNumberTypes(List<CallNumberType> callNumberTypes) {
    this.callNumberTypes = new UnmodifiableList<>(callNumberTypes);
    return this;
  }

  public MappingParameters withStatisticalCodes(List<StatisticalCode> statisticalCodes) {
    this.statisticalCodes = new UnmodifiableList<>(statisticalCodes);
    return this;
  }

  public MappingParameters withStatisticalCodeTypes(List<StatisticalCodeType> statisticalCodeTypes) {
    this.statisticalCodeTypes = new UnmodifiableList<>(statisticalCodeTypes);
    return this;
  }

  public MappingParameters withLocations(List<Location> locations) {
    this.locations = new UnmodifiableList<>(locations);
    return this;
  }

  public MappingParameters withMaterialTypes(List<Mtype> materialTypes) {
    this.materialTypes = new UnmodifiableList<>(materialTypes);
    return this;
  }

  public MappingParameters withItemDamagedStatuses(List<ItemDamageStatus> itemDamageStatuses) {
    this.itemDamageStatuses = new UnmodifiableList<>(itemDamageStatuses);
    return this;
  }

  public MappingParameters withLoanTypes(List<Loantype> loantypes) {
    this.loanTypes = new UnmodifiableList<>(loantypes);
    return this;
  }

  public MappingParameters withItemNoteTypes(List<ItemNoteType> itemNoteTypes) {
    this.itemNoteTypes = new UnmodifiableList<>(itemNoteTypes);
    return this;
  }

  public MappingParameters withTenantConfiguration(String tenantConfiguration) {
    this.tenantConfiguration = tenantConfiguration;
    return this;
  }

  public MappingParameters withMarcFieldProtectionSettings(List<MarcFieldProtectionSetting> marcFieldProtectionSettings) {
    this.marcFieldProtectionSettings = new UnmodifiableList<>(marcFieldProtectionSettings);
    return this;
  }

  public MappingParameters withAuthorityNoteTypes(List<AuthorityNoteType> authorityNoteTypes) {
    this.authorityNoteTypes = Collections.unmodifiableList(authorityNoteTypes);
    return this;
  }

  public MappingParameters withAuthoritySourceFiles(List<AuthoritySourceFile> authoritySourceFiles) {
    this.authoritySourceFiles = Collections.unmodifiableList(authoritySourceFiles);
    return this;
  }
  public List<Organization> getOrganizations() {
    return organizations;
  }

  public void setOrganizations(List<Organization> organizations) {
    this.organizations = organizations;
  }

  public MappingParameters withOrganizations(List<Organization> organizations) {
    this.organizations = new UnmodifiableList<>(organizations);
    return this;
  }
}
