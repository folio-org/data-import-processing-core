package org.folio.processing.mapping.mapper.writer.marc;

import static java.util.Collections.emptyList;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.folio.DataImportEventPayload;
import org.folio.InstanceLinkDtoCollection;
import org.folio.Link;
import org.folio.MappingProfile;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.EntityType;
import org.marc4j.marc.DataField;

public class MarcBibRecordModifier extends MarcRecordModifier {

  private static final char SUBFIELD_0 = '0';
  private static final char SUBFIELD_9 = '9';

  private List<Link> bibAuthorityLinks = emptyList();
  private final Set<Link> bibAuthorityLinksKept = new HashSet<>();

  public List<Link> getBibAuthorityLinksKept() {
    return new LinkedList<>(bibAuthorityLinksKept);
  }

  public void initialize(DataImportEventPayload eventPayload, MappingParameters mappingParameters,
                         MappingProfile mappingProfile, EntityType marcType,
                         InstanceLinkDtoCollection links) throws IOException {
    validateEntityType(marcType);
    initialize(eventPayload, mappingParameters, mappingProfile, marcType);
    if (links == null || links.getLinks() == null) {
      return;
    }
    bibAuthorityLinks = links.getLinks();
  }

  /**
   * Should call regular update subfield flow only for uncontrolled subfields
   * */
  @Override
  protected boolean updateSubfields(String subfieldCode, List<DataField> tmpFields, DataField fieldToUpdate,
                                    DataField fieldReplacement, boolean ifNewDataShouldBeAdded) {
    var linkOptional = getLink(fieldToUpdate);
    if (linkOptional.isPresent() && fieldsLinked(linkOptional.get(), fieldReplacement, fieldToUpdate)) {
      var link = linkOptional.get();
      bibAuthorityLinksKept.add(link);
      return updateUncontrolledSubfields(link, subfieldCode, tmpFields, fieldToUpdate, fieldReplacement);
    }

    removeSubfield9(fieldToUpdate, fieldReplacement);
    if (subfieldCode.charAt(0) == SUBFIELD_9) {
      return false;
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
      .filter(link -> link.getBibRecordTag().equals(dataField.getTag()))
      .findFirst();
  }

  private boolean subfieldLinked(Link link, String subfieldCode) {
    return link.getBibRecordSubfields().contains(subfieldCode)
      || subfieldCode.charAt(0) == SUBFIELD_0
      || subfieldCode.charAt(0) == SUBFIELD_9;
  }

  /**
   * Indicates that incoming and existing fields hold the same link
   * */
  private boolean fieldsLinked(Link link, DataField incomingField, DataField fieldToChange) {
    var incomingSubfield0 = incomingField.getSubfield(SUBFIELD_0);
    var existingSubfield0 = fieldToChange.getSubfield(SUBFIELD_0);
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
      .filter(link -> link.getBibRecordTag().equals(dataField.getTag()))
      .anyMatch(link -> subfield0.getData().endsWith(link.getAuthorityNaturalId()));
  }

  private void validateEntityType(EntityType entityType) {
    if (!entityType.equals(EntityType.MARC_BIBLIOGRAPHIC)) {
      throw new IllegalArgumentException(this.getClass().getSimpleName() + " support only "
        + EntityType.MARC_BIBLIOGRAPHIC.value());
    }
  }
}
