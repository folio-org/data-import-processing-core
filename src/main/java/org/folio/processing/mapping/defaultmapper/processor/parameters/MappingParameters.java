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
  private List<ElectronicAccessRelationship> electronicAccessRelationship = new ArrayList<>();
  private List<InstanceFormat> instanceFormats = new ArrayList<>();
  private List<ContributorType> contributorTypes = new ArrayList<>();
  private List<ContributorNameType> contributorNameTypes = new ArrayList<>();
  private List<InstanceNoteType> instanceNoteTypes = new ArrayList<>();
  private List<AlternativeTitleType> alternativeTitleTypes = new ArrayList<>();
  private List<IssuanceMode> issuanceModes = new ArrayList<>();

  public boolean isInitialized() {
    return initialized;
  }

  public MappingParameters withInitializedState(boolean initialized) {
    this.initialized = initialized;
    return this;
  }

  public List<ElectronicAccessRelationship> getElectronicAccessRelationship() {
    return electronicAccessRelationship;
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

  public List<ElectronicAccessRelationship> getElectronicAccessRelationships() {
    return electronicAccessRelationship;
  }

  public MappingParameters withElectronicAccessRelationships(List<ElectronicAccessRelationship> electronicAccessRelationship) {
    this.electronicAccessRelationship = new UnmodifiableList<>(electronicAccessRelationship);
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
}
