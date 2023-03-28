package org.folio.processing.mapping.mapper.writer.marc;

import static java.util.Collections.emptyList;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
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
  private static final List<String> LINKABLE_TAGS = List.of("100", "110", "111", "130", "240", "600", "610",
    "611", "630", "700", "710", "711", "730", "800", "810", "811", "830");

  private List<Link> bibAuthorityLinks = emptyList();
  private List<LinkingRuleDto> linkingRules = emptyList();
  private final Set<Link> bibAuthorityLinksKept = new HashSet<>();

  public List<Link> getBibAuthorityLinksKept() {
    return new LinkedList<>(bibAuthorityLinksKept);
  }

  public void initialize(DataImportEventPayload eventPayload, MappingParameters mappingParameters,
    MappingProfile mappingProfile, EntityType marcType,
    InstanceLinkDtoCollection links, List<LinkingRuleDto> linkingRules) throws IOException {
    validateEntityType(marcType);
    initialize(eventPayload, mappingParameters, mappingProfile, marcType);
    if (links == null || links.getLinks() == null) {
      return;
    }
    bibAuthorityLinks = links.getLinks();
    this.linkingRules = linkingRules;
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
    if (linkOptional.isPresent() && fieldsLinked(subfieldCode.charAt(0), linkOptional.get(), fieldReplacement, fieldToUpdate)) {
      var link = linkOptional.get();
      bibAuthorityLinksKept.add(link);
      return updateUncontrolledSubfields(link, subfieldCode, tmpFields, fieldToUpdate, fieldReplacement);
    }

    if (LINKABLE_TAGS.contains(fieldReplacement.getTag())) {
      removeSubfield9(fieldToUpdate, fieldReplacement);
      if (subfieldCode.charAt(0) == SUBFIELD_9) {
        return false;
      }
    }

    return super.updateSubfields(subfieldCode, tmpFields, fieldToUpdate, fieldReplacement, ifNewDataShouldBeAdded);
  }

  @Override
  protected boolean unUpdatedFieldShouldBeRemoved(DataField dataField) {
    return super.unUpdatedFieldShouldBeRemoved(dataField) && !fieldLinked(dataField);
  }

  private boolean updateUncontrolledSubfields(Link link, String subfieldCode, List<DataField> tmpFields,
                                              DataField fieldToUpdate, DataField fieldReplacement) {
    if (subfieldCode.equals(ANY_STRING)) {
      fieldReplacement.getSubfields()
        .forEach(subfield ->
          updateUncontrolledSubfield(link, String.valueOf(subfield.getCode()), tmpFields, fieldToUpdate, fieldReplacement));
      removeUncontrolledNotUpdatedSubfields(link, fieldToUpdate, fieldReplacement);
    } else {
      updateUncontrolledSubfield(link, subfieldCode, tmpFields, fieldToUpdate, fieldReplacement);
    }

    return false;
  }

  private void updateUncontrolledSubfield(Link link, String subfieldCode, List<DataField> tmpFields,
                                          DataField fieldToUpdate, DataField fieldReplacement) {
    if (subfieldLinked(link, subfieldCode)) {
      return;
    }

    super.updateSubfields(subfieldCode, tmpFields, fieldToUpdate, fieldReplacement, false);
  }

  private void removeUncontrolledNotUpdatedSubfields(Link link, DataField fieldToUpdate, DataField fieldReplacement) {
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
    fieldToUpdate.getSubfields().removeIf(subfield -> subfield.getCode() == SUBFIELD_9);
    fieldReplacement.getSubfields().removeIf(subfield -> subfield.getCode() == SUBFIELD_9);
  }

  private Optional<Link> getLink(DataField dataField) {
    return bibAuthorityLinks.stream()
      .filter(link -> linkingRules.stream()
        .filter(linkingRuleDto -> linkingRuleDto.getBibField().equals(dataField.getTag()))
        .map(LinkingRuleDto::getId)
        .collect(Collectors.toList())
        .contains(link.getLinkingRuleId()))
      .filter(link -> {
        var sub9Matches = Optional.ofNullable(dataField.getSubfield(SUBFIELD_9))
          .map(subfield -> subfield.getData().equalsIgnoreCase(link.getAuthorityId()))
          .orElse(false);
        var sub0Matches = Optional.ofNullable(dataField.getSubfield(SUBFIELD_0))
          .map(subfield -> StringUtils.endsWithIgnoreCase(subfield.getData(), link.getAuthorityNaturalId()))
          .orElse(false);
        return sub9Matches || sub0Matches;
      })
      .findFirst();
  }

  private boolean subfieldLinked(Link link, String subfieldCode) {
    Optional<LinkingRuleDto> ruleDto = linkingRules.stream().filter(r -> r.getId().equals(link.getLinkingRuleId())).findFirst();
    if (ruleDto.isEmpty()) {
      return false;
    }

    LinkingRuleDto dto = ruleDto.get();
    List<String> bibControlledSubfields = dto.getAuthoritySubfields();
    List<SubfieldModification> subfieldModifications = dto.getSubfieldModifications();

    bibControlledSubfields = bibControlledSubfields.stream()
      .map(s -> subfieldModifications.stream()
        .filter(subfieldModification -> s.equals(subfieldModification.getSource()))
        .map(SubfieldModification::getTarget)
        .findFirst()
        .orElse(s))
      .collect(Collectors.toList());
    return bibControlledSubfields.contains(subfieldCode)
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

  /**
   * Indicates that field is still linked after update
   * */
  private boolean fieldLinked(DataField dataField) {
    var subfield0 = dataField.getSubfield(SUBFIELD_0);
    if (subfield0 == null) {
      return false;
    }

    return bibAuthorityLinksKept.stream()
      .filter(link -> linkingRules.stream()
        .map(LinkingRuleDto::getId)
        .collect(Collectors.toList())
        .contains(link.getLinkingRuleId()))
      .anyMatch(link -> subfield0.getData().endsWith(link.getAuthorityNaturalId()));
  }

  private void validateEntityType(EntityType entityType) {
    if (!entityType.equals(EntityType.MARC_BIBLIOGRAPHIC)) {
      throw new IllegalArgumentException(this.getClass().getSimpleName() + " support only "
        + EntityType.MARC_BIBLIOGRAPHIC.value());
    }
  }
}
