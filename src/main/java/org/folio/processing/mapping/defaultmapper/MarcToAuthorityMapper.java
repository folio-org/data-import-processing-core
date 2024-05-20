package org.folio.processing.mapping.defaultmapper;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.Authority;
import org.folio.AuthoritySourceFile;
import org.folio.processing.mapping.defaultmapper.processor.Processor;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.marc4j.MarcJsonReader;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;

public class MarcToAuthorityMapper implements RecordMapper<Authority> {

  private static final String MARC_FORMAT = "MARC_AUTHORITY";
  private static final Pattern ALPHABETIC_PREFIX_PATTERN = Pattern.compile("[a-zA-Z]+");

  private final HashMap<String, String> sourceFileIdsByPrefix = new HashMap<>();

  @Override
  public Authority mapRecord(JsonObject parsedRecord, MappingParameters mappingParameters, JsonObject mappingRules) {
    var authority = new Processor<Authority>().process(parsedRecord, mappingParameters, mappingRules, Authority.class);

    linkSourceFile(parsedRecord, mappingParameters, authority);

    return authority;
  }

  @Override
  public List<Authority> mapRecords(List<JsonObject> parsedRecords,
                                    MappingParameters mappingParameters,
                                    JsonObject mappingRules) {
    List<Authority> authorities = new ArrayList<>();
    for (var parsedRecord : parsedRecords) {
      var reader = new MarcJsonReader(new ByteArrayInputStream(parsedRecord.toString().getBytes(UTF_8)));
      if (reader.hasNext()) {
        var marcRecord = reader.next();
        var authority = new Processor<Authority>().process(marcRecord, mappingParameters, mappingRules, Authority.class);
        linkSourceFile(parsedRecord, mappingParameters, authority);
        authorities.add(authority);
      }
    }

    return authorities;
  }

  @Override
  public String getMapperFormat() {
    return MARC_FORMAT;
  }

  private void linkSourceFile(JsonObject parsedRecord, MappingParameters mappingParameters, Authority authority) {
    var sourceFiles = mappingParameters.getAuthoritySourceFiles();
    if (sourceFiles == null || sourceFiles.isEmpty()) {
      return;
    }

    var reader = new MarcJsonReader(new ByteArrayInputStream(parsedRecord.toString().getBytes(UTF_8)));
    if (reader.hasNext()) {
      findAndLinkSourceFile(authority, sourceFiles, reader.next());
    }
  }

  private void findAndLinkSourceFile(Authority authority, List<AuthoritySourceFile> sourceFiles, Record marcRecord) {
    String sourceFileId = null;
    String naturalId = null;

    var tag010ASubfieldValues = getTag010ASubfieldValues(marcRecord);
    for (var aSubfieldValue : tag010ASubfieldValues) {
      if ((sourceFileId = findSourceFileByTagValue(sourceFiles, aSubfieldValue)) != null) {
        naturalId = aSubfieldValue;
        break;
      }
    }

    if (sourceFileId == null) {
      var tag001Value = getTag001Value(marcRecord);

      sourceFileId = findSourceFileByTagValue(sourceFiles, tag001Value);
      naturalId = tag001Value;
    }

    authority.setSourceFileId(sourceFileId);
    authority.setNaturalId(sanitizedAlphaNumericValue(naturalId));
  }

  private List<String> getTag010ASubfieldValues(Record marcRecord) {
    return marcRecord.getDataFields().stream().filter(f -> f.getTag().equals("010"))
      .map(tag -> tag.getSubfields('a'))
      .flatMap(List::stream)
      .map(Subfield::getData)
      .toList();
  }

  private String getTag001Value(Record marcRecord) {
    return marcRecord.getControlFields().stream().filter(f -> f.getTag().equals("001"))
      .findFirst()
      .map(ControlField::getData)
      .orElse(null);
  }

  private String findSourceFileByTagValue(List<AuthoritySourceFile> sourceFiles, String value) {
    if (value == null) {
      return null;
    }
    var sourceFilePrefix = getSanitizedAlphaNumericPrefix(value);
    if (sourceFilePrefix == null) {
      return null;
    }
    if (sourceFileIdsByPrefix.containsKey(sourceFilePrefix)) {
      return sourceFileIdsByPrefix.get(sourceFilePrefix);
    }

    var codeIdsMap = sourceFiles.stream().map(file -> {
        var id = file.getId();
        return file.getCodes().stream().collect(Collectors.toMap(code -> code, code -> id));
      }).flatMap(map -> map.entrySet().stream())
      .sorted(Comparator.comparing(codeIdEntry -> -codeIdEntry.getKey().length()))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
        (v1, v2) -> v2,
        LinkedHashMap::new));

    var sourceFileId = codeIdsMap.entrySet().stream()
      .filter(codeIdEntry -> StringUtils.equals(codeIdEntry.getKey(), sourceFilePrefix))
      .map(Map.Entry::getValue)
      .findFirst()
      .orElse(null);

    if (sourceFileId != null) {
      sourceFileIdsByPrefix.put(sourceFilePrefix, sourceFileId);
    }

    return sourceFileId;
  }

  private String getSanitizedAlphaNumericPrefix(String value) {
    if (value == null) {
      return null;
    }
    var sanitizedTagValue = sanitizedAlphaNumericValue(value);
    var matcher = ALPHABETIC_PREFIX_PATTERN.matcher(sanitizedTagValue);
    if (!matcher.lookingAt()) {
      // tag value does not start with alphabet letters
      return null;
    }
    return sanitizedTagValue.substring(matcher.start(), matcher.end());
  }

  private static String sanitizedAlphaNumericValue(String str) {
    return str == null ? null : str.replaceAll("[^0-9a-zA-Z]", "");
  }

}
