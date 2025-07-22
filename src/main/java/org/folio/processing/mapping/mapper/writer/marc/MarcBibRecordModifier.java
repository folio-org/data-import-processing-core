package org.folio.processing.mapping.mapper.writer.marc;

import static java.util.Collections.emptyList;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.InstanceLinkDtoCollection;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.MappingProfile;
import org.folio.SubfieldModification;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.EntityType;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Subfield;

public class MarcBibRecordModifier extends MarcRecordModifier {

  private static final char SUBFIELD_0 = '0';
  private static final char SUBFIELD_9 = '9';

  private List<LinkFull> bibAuthorityLinks = emptyList();
  private List<LinkingRuleDto> linkingRules = emptyList();
  private final Set<LinkFull> bibAuthorityLinksKept = new HashSet<>();

  public List<Link> getBibAuthorityLinksKept() {
    return bibAuthorityLinksKept.stream()
      .map(LinkFull::getLink)
      .collect(Collectors.toList());
  }

  public void initialize(DataImportEventPayload eventPayload, MappingParameters mappingParameters,
    MappingProfile mappingProfile, EntityType marcType,
    InstanceLinkDtoCollection links, List<LinkingRuleDto> linkingRules) throws IOException {
    validateEntityType(marcType);
    initialize(eventPayload, mappingParameters, mappingProfile, marcType);
    if (links == null || links.getLinks() == null) {
      return;
    }
    bibAuthorityLinks = buildFullLinks(links.getLinks(), linkingRules);
    this.linkingRules = linkingRules;
  }

  public void setLinks(InstanceLinkDtoCollection links, List<LinkingRuleDto> linkingRules) {
    bibAuthorityLinks = buildFullLinks(links.getLinks(), linkingRules);
    this.linkingRules = linkingRules;
  }

  @Override
  protected void addNewUpdatedField(DataField fieldReplacement) {
    if (containsBibTag(linkingRules, fieldReplacement.getTag()) && getLink(fieldReplacement).isEmpty()) {
      removeSubfield9(fieldReplacement);
    }
    super.addNewUpdatedField(fieldReplacement);
  }

  @Override
  protected void doAdditionalProtectedFieldAction(DataField fieldToUpdate) {
    getLink(fieldToUpdate).ifPresent(bibAuthorityLinksKept::add);
  }

  /**
   * Should call regular update subfield flow only for uncontrolled subfields
   * */
  @Override
  protected boolean updateSubfields(String subfieldCode, List<DataField> tmpFields, DataField fieldToUpdate,
                                    DataField fieldReplacement, boolean ifNewDataShouldBeAdded) {
    var linkOptional = getLink(fieldToUpdate);
    if (linkOptional.isPresent() && fieldsLinked(subfieldCode.charAt(0), linkOptional.get().getLink(), fieldReplacement, fieldToUpdate)) {
      var link = linkOptional.get();
      bibAuthorityLinksKept.add(link);
      return updateUncontrolledSubfields(link, subfieldCode, tmpFields, fieldToUpdate, fieldReplacement);
    }

    if (containsBibTag(linkingRules, fieldReplacement.getTag())) {
      removeSubfield9(fieldToUpdate, fieldReplacement);
      if (subfieldCode.charAt(0) == SUBFIELD_9) {
        return false;
      }
    }

    return super.updateSubfields(subfieldCode, tmpFields, fieldToUpdate, fieldReplacement, ifNewDataShouldBeAdded);
  }

  @Override
  protected void clearDataField(DataField updatedField) {
    removeSubfield9(updatedField);
    super.clearDataField(updatedField);
  }

  @Override
  protected boolean unUpdatedFieldShouldBeRemoved(DataField dataField) {
    return super.unUpdatedFieldShouldBeRemoved(dataField) && !fieldLinked(dataField);
  }

  @Override
  protected boolean fieldsDeepMatch(List<DataField> fieldReplacements, List<DataField> fieldsToUpdate,
                                    DataField fieldReplacement, DataField fieldToUpdate) {
    if (isNonRepeatableField(fieldReplacement)) {
      return true;
    }

    var incomingSubfields0 = fieldReplacement.getSubfields(SUBFIELD_0).stream()
      .map(Subfield::getData)
      .collect(Collectors.toList());
    // only one subfield $0 extracted from existing because linked field can only have single $0
    var existingSubfield0 = fieldToUpdate.getSubfield(SUBFIELD_0);

    // both absent or both have equal $0
    if (incomingSubfields0.isEmpty() && existingSubfield0 == null
      || existingSubfield0 != null && incomingSubfields0.contains(existingSubfield0.getData())) {
      return true;
    }

    //if incoming linked to some other existing
    var incomingFieldLinked = fieldsToUpdate.stream()
      .anyMatch(dataField -> dataField.getTag().equals(fieldReplacement.getTag())
        && dataField.getSubfield(SUBFIELD_0) != null
        && incomingSubfields0.contains(dataField.getSubfield(SUBFIELD_0).getData()));

    if (existingSubfield0 == null) {
      return !incomingFieldLinked;
    }

    //if existing linked to some other incoming
    var existingFieldLinked = fieldReplacements.stream()
      .anyMatch(dataField -> dataField.getTag().equals(fieldToUpdate.getTag())
        && dataField.getSubfield(SUBFIELD_0) != null
        && existingSubfield0.getData().equals(dataField.getSubfield(SUBFIELD_0).getData()));

    return !(incomingFieldLinked || existingFieldLinked);
  }

  private boolean updateUncontrolledSubfields(LinkFull link, String subfieldCode, List<DataField> tmpFields,
                                              DataField fieldToUpdate, DataField fieldReplacement) {
    if (subfieldCode.equals(ANY_STRING)) {
      fieldReplacement.getSubfields()
        .forEach(subfield ->
          updateUncontrolledSubfield(link, String.valueOf(subfield.getCode()), tmpFields, fieldToUpdate, fieldReplacement));
      removeUncontrolledNotUpdatedSubfields(link, fieldToUpdate, fieldReplacement);
      fieldReplacement.getSubfields().clear();
      fieldReplacement.getSubfields().addAll(fieldToUpdate.getSubfields());
      return super.updateSubfields(subfieldCode, tmpFields, fieldToUpdate, fieldReplacement, true);
    } else {
      updateUncontrolledSubfield(link, subfieldCode, tmpFields, fieldToUpdate, fieldReplacement);
    }

    return false;
  }

  private void updateUncontrolledSubfield(LinkFull link, String subfieldCode, List<DataField> tmpFields,
                                          DataField fieldToUpdate, DataField fieldReplacement) {
    if (subfieldLinked(link, subfieldCode)) {
      return;
    }

    super.updateSubfields(subfieldCode, tmpFields, fieldToUpdate, fieldReplacement, false);
  }

  private void removeUncontrolledNotUpdatedSubfields(LinkFull link, DataField fieldToUpdate, DataField fieldReplacement) {
    fieldToUpdate.getSubfields().stream()
      .filter(subfield -> !subfieldLinked(link, String.valueOf(subfield.getCode())))
      .filter(subfield -> fieldReplacement.getSubfields().stream()
        .noneMatch(subfieldReplacement -> subfield.getCode() == subfieldReplacement.getCode()))
      .collect(Collectors.toList())
      .forEach(fieldToUpdate::removeSubfield);
  }

  /**
   * Removes subfield 9 from field.
   * It's not enough to remove it only from incoming field.
   * It also should be removed if $9 is not in mapping details (and subfield is not '*') but field is unlinked.
   * */
  private void removeSubfield9(DataField fieldToUpdate, DataField fieldReplacement) {
    removeSubfield9(fieldToUpdate);
    removeSubfield9(fieldReplacement);
  }
  private void removeSubfield9(DataField dataField) {
    dataField.getSubfields().removeIf(subfield -> subfield.getCode() == SUBFIELD_9);
  }

  private Optional<LinkFull> getLink(DataField dataField) {
    return bibAuthorityLinks.stream()
      .filter(link -> link.getBibTag().equals(dataField.getTag()))
      .filter(link -> {
        var sub9Matches = Optional.ofNullable(dataField.getSubfield(SUBFIELD_9))
          .map(subfield -> subfield.getData().equalsIgnoreCase(link.getLink().getAuthorityId()))
          .orElse(false);
        var sub0Matches = Optional.ofNullable(dataField.getSubfield(SUBFIELD_0))
          .map(subfield -> StringUtils.endsWithIgnoreCase(subfield.getData(), link.getLink().getAuthorityNaturalId()))
          .orElse(false);
        return sub9Matches || sub0Matches;
      })
      .findFirst();
  }

  private boolean subfieldLinked(LinkFull link, String subfieldCode) {
    return link.getBibSubfields().contains(subfieldCode)
      || subfieldCode.charAt(0) == SUBFIELD_0
      || subfieldCode.charAt(0) == SUBFIELD_9;
  }

  /**
   * Indicates that incoming and existing fields hold the same link.
   * If subfieldCode is '*' - no mapping rules exists - take $0 from incoming field
   * If subfieldCode is '0' - mapping rules exists that say that only $0 could be updated - take $0 from incoming field
   * If subfieldCode is any other - mapping rules exists that say that subfield could be updated - take $0 from existing field, as it is not expected to be updated
   * If there are more than one $0 exist in incoming field - at least one should match with existing field and link,
   * all other will be removed during field update.
   * */
  private boolean fieldsLinked(char subfieldCode, Link link, DataField incomingField, DataField fieldToChange) {
    List<Subfield> incomingSubfields0;
    if (subfieldCode == ANY_CHAR || subfieldCode == SUBFIELD_0) {
      incomingSubfields0 = incomingField.getSubfields(SUBFIELD_0);
    } else {
      incomingSubfields0 = Collections.singletonList(fieldToChange.getSubfield(SUBFIELD_0));
    }
    var existingSubfield0 = fieldToChange.getSubfield(SUBFIELD_0);
    for (Subfield incomingSubfield0 : incomingSubfields0) {
      if (hasSameLink(link, existingSubfield0, incomingSubfield0)) {
        return true;
      }
    }
    return false;
  }

  private boolean hasSameLink(Link link, Subfield existingSubfield0, Subfield incomingSubfield0) {
    return incomingSubfield0 != null
      && existingSubfield0 != null
      && incomingSubfield0.getData().endsWith(link.getAuthorityNaturalId())
      && incomingSubfield0.getData().equals(existingSubfield0.getData());
  }

  private boolean containsBibTag(List<LinkingRuleDto> linkingRules, String tag) {
    return linkingRules.stream()
      .map(LinkingRuleDto::getBibField)
      .anyMatch(bibField -> bibField.equals(tag));
  }

  /**
   * Indicates that field is still linked after update
   * */
  private boolean fieldLinked(DataField dataField) {
    var subfield0 = dataField.getSubfield(SUBFIELD_0);
    if (subfield0 == null) {
      return false;
    }

    return bibAuthorityLinksKept.stream()
      .filter(link -> link.getBibTag().equals(dataField.getTag()))
      .anyMatch(link -> subfield0.getData().endsWith(link.getLink().getAuthorityNaturalId()));
  }

  private void validateEntityType(EntityType entityType) {
    if (!entityType.equals(EntityType.MARC_BIBLIOGRAPHIC)) {
      throw new IllegalArgumentException(this.getClass().getSimpleName() + " support only "
        + EntityType.MARC_BIBLIOGRAPHIC.value());
    }
  }

  private List<LinkFull> buildFullLinks(List<Link> links, List<LinkingRuleDto> linkingRules) {
    return links.stream()
      .map(link -> {
        var linkingRule = linkingRules.stream()
          .filter(linkingRuleDto -> linkingRuleDto.getId().equals(link.getLinkingRuleId()))
          .findFirst();

        if (linkingRule.isEmpty()) {
          return null;
        }

        var subfieldModifications = linkingRule.get().getSubfieldModifications();
        var bibSubfields = linkingRule.get().getAuthoritySubfields().stream()
          .map(s -> subfieldModifications.stream()
            .filter(subfieldModification -> s.equals(subfieldModification.getSource()))
            .map(SubfieldModification::getTarget)
            .findFirst()
            .orElse(s))
          .collect(Collectors.toList());

        return new LinkFull(link, linkingRule.get().getBibField(), bibSubfields);
      })
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }
}
