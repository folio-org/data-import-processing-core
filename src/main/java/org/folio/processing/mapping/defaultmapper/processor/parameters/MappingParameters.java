package org.folio.processing.mapping.defaultmapper.processor.parameters;

import org.apache.commons.collections4.list.UnmodifiableList;
import org.folio.AlternativeTitleType;
import org.folio.ClassificationType;
import org.folio.ContributorNameType;
import org.folio.ContributorType;
import org.folio.ElectronicAccessRelationship;
import org.folio.IdentifierType;
import org.folio.InstanceFormat;
import org.folio.InstanceNoteType;
import org.folio.InstanceType;
import org.folio.IssuanceMode;

import java.util.ArrayList;
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
}