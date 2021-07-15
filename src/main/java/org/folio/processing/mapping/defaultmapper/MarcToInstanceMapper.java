package org.folio.processing.mapping.defaultmapper;

import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.Classification;
import org.folio.ElectronicAccess;
import org.folio.Identifier;
import org.folio.Identifier_;
import org.folio.Identifier__;
import org.folio.Instance;
import org.folio.PrecedingTitle;
import org.folio.SucceedingTitle;
import org.folio.processing.mapping.defaultmapper.processor.Processor;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class MarcToInstanceMapper implements RecordToInstanceMapper {

  private static final Pattern UUID_DUPLICATE_PATTERN = Pattern.compile("([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} ){2,}");
  private static final String BLANK_STRING = " ";

  @Override
  public Instance mapRecord(JsonObject parsedRecord, MappingParameters mappingParameters, JsonObject mappingRules) {
    Instance instance = new Processor().process(parsedRecord, mappingParameters, mappingRules);
    if (instance != null) {
      instance = fixDuplicatedUUIDs(instance.withSource(getMapperFormat()));
      instance = fixDuplicatedLanguages(instance);
      instance = removeElectronicAccessEntriesWithNoUri(instance);
      instance = removePrecedingTitlesWithoutTitles(instance);
      instance = removeSucceedingTitlesWithoutTitles(instance);
      instance = movePrecedingIdentifiers(instance);
      instance = moveSucceedingIdentifiers(instance);
    }
    return instance;
  }

  @Override
  public String getMapperFormat() {
    return "MARC";
  }

  private Instance fixDuplicatedUUIDs(Instance instance) {
    fixIdentifiers(instance);
    fixClassifications(instance);
    return instance;
  }

  private void fixIdentifiers(Instance instance) {
    List<Identifier> splitIdentifiers = new ArrayList<>();
    instance.getIdentifiers().forEach(identifier -> {
      if (StringUtils.isNoneBlank(identifier.getIdentifierTypeId())
        && UUID_DUPLICATE_PATTERN.matcher(identifier.getIdentifierTypeId() + BLANK_STRING).matches()) {
        String[] uuids = identifier.getIdentifierTypeId().split(BLANK_STRING);
        String[] values = identifier.getValue().split(BLANK_STRING);
        if (uuids.length > 1 && values.length > 1) {
          identifier.setIdentifierTypeId(uuids[0]);
          identifier.setValue(values[0]);
        }
        for (int i = 1; i < uuids.length; i++) {
          Identifier newIdentifier = new Identifier().withIdentifierTypeId(uuids[i]);
          if (values.length > i) {
            newIdentifier.setValue(i == uuids.length - 1
              ? String.join(BLANK_STRING, Arrays.copyOfRange(values, i, values.length))
              : values[i]);
            splitIdentifiers.add(newIdentifier);
          }
        }
      }
    });
    instance.getIdentifiers().addAll(splitIdentifiers);
  }

  private void fixClassifications(Instance instance) {
    List<Classification> splitClassification = new ArrayList<>();
    instance.getClassifications().forEach(classification -> {
      if (StringUtils.isNoneBlank(classification.getClassificationTypeId())
        && UUID_DUPLICATE_PATTERN.matcher(classification.getClassificationTypeId() + BLANK_STRING).matches()) {
        String[] uuids = classification.getClassificationTypeId().split(BLANK_STRING);
        String[] values = classification.getClassificationNumber().split(BLANK_STRING);
        if (uuids.length > 1 && values.length > 1) {
          classification.setClassificationTypeId(uuids[0]);
          classification.setClassificationNumber(values[0]);
        }
        for (int i = 1; i < uuids.length; i++) {
          Classification newClassification = new Classification().withClassificationTypeId(uuids[i]);
          if (values.length > i) {
            newClassification.setClassificationNumber(i == uuids.length - 1
              ? String.join(BLANK_STRING, Arrays.copyOfRange(values, i, values.length))
              : values[i]);
            splitClassification.add(newClassification);
          }
        }
      }
    });
    instance.getClassifications().addAll(splitClassification);
  }

  private Instance fixDuplicatedLanguages(Instance instance) {
    List<String> uniqueLanguages = instance.getLanguages().stream()
      .distinct()
      .collect(Collectors.toList());
    return instance.withLanguages(uniqueLanguages);
  }

  private Instance removeElectronicAccessEntriesWithNoUri(Instance instance) {
    List<ElectronicAccess> electronicAccessList = instance.getElectronicAccess().stream()
      .filter(electronicAccess -> isNotEmpty(electronicAccess.getUri()))
      .collect(Collectors.toList());
    return instance.withElectronicAccess(electronicAccessList);
  }

  private Instance removePrecedingTitlesWithoutTitles(Instance instance) {
    List<PrecedingTitle> precedingTitles = instance.getPrecedingTitles().stream()
      .filter(precedingTitle -> isNotEmpty(precedingTitle.getTitle()))
      .collect(Collectors.toList());
    return instance.withPrecedingTitles(precedingTitles);
  }

  private Instance removeSucceedingTitlesWithoutTitles(Instance instance) {
    List<SucceedingTitle> succeedingTitles = instance.getSucceedingTitles().stream()
      .filter(succeedingTitle -> isNotEmpty(succeedingTitle.getTitle()))
      .collect(Collectors.toList());
    return instance.withSucceedingTitles(succeedingTitles);
  }

  private Instance movePrecedingIdentifiers(Instance instance) {
    List<PrecedingTitle> precedingTitles = instance.getPrecedingTitles().stream()
      .peek(precedingTitle -> {
        if (isNotEmpty(precedingTitle.getIsbnId())) {
          Identifier_ identifier = new Identifier_().withIdentifierTypeId(precedingTitle.getIsbnId());
          precedingTitle.setIsbnId(null);
          if (isNotEmpty(precedingTitle.getIsbnValue())) {
            identifier.setValue(precedingTitle.getIsbnValue());
            precedingTitle.setIsbnValue(null);
            precedingTitle.getIdentifiers().add(identifier);
          }
        }
        if (isNotEmpty(precedingTitle.getIssnId())) {
          Identifier_ identifier = new Identifier_().withIdentifierTypeId(precedingTitle.getIssnId());
          precedingTitle.setIssnId(null);
          if (isNotEmpty(precedingTitle.getIssnValue())) {
            identifier.setValue(precedingTitle.getIssnValue());
            precedingTitle.setIssnValue(null);
            precedingTitle.getIdentifiers().add(identifier);
          }
        }
      })
      .collect(Collectors.toList());
    return instance.withPrecedingTitles(precedingTitles);
  }

  private Instance moveSucceedingIdentifiers(Instance instance) {
    List<SucceedingTitle> succeedingTitles = instance.getSucceedingTitles().stream()
      .peek(succeedingTitle -> {
        if (isNotEmpty(succeedingTitle.getIsbnId())) {
          Identifier__ identifier = new Identifier__().withIdentifierTypeId(succeedingTitle.getIsbnId());
          succeedingTitle.setIsbnId(null);
          if (isNotEmpty(succeedingTitle.getIsbnValue())) {
            identifier.setValue(succeedingTitle.getIsbnValue());
            succeedingTitle.setIsbnValue(null);
            succeedingTitle.getIdentifiers().add(identifier);
          }
        }
        if (isNotEmpty(succeedingTitle.getIssnId())) {
          Identifier__ identifier = new Identifier__().withIdentifierTypeId(succeedingTitle.getIssnId());
          succeedingTitle.setIssnId(null);
          if (isNotEmpty(succeedingTitle.getIssnValue())) {
            identifier.setValue(succeedingTitle.getIssnValue());
            succeedingTitle.setIssnValue(null);
            succeedingTitle.getIdentifiers().add(identifier);
          }
        }
      })
      .collect(Collectors.toList());
    return instance.withSucceedingTitles(succeedingTitles);
  }
}
