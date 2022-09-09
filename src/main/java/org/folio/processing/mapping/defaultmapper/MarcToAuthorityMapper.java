package org.folio.processing.mapping.defaultmapper;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

  @Override
  public Authority mapRecord(JsonObject parsedRecord, MappingParameters mappingParameters, JsonObject mappingRules) {
    var authority = new Processor<Authority>().process(parsedRecord, mappingParameters, mappingRules, Authority.class);

    linkSourceFile(parsedRecord, mappingParameters, authority);

    return authority;
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
    authority.setNaturalId(removeWhitespaces(naturalId));
  }

  private List<String> getTag010ASubfieldValues(Record marcRecord) {
    return marcRecord.getDataFields().stream().filter(f -> f.getTag().equals("010"))
      .map(tag -> tag.getSubfields('a'))
      .flatMap(List::stream)
      .map(Subfield::getData)
      .collect(Collectors.toList());
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

    var codeIdsMap = sourceFiles.stream().map(file -> {
        var id = file.getId();
        return file.getCodes().stream().collect(Collectors.toMap(code -> code, code -> id));
      }).flatMap(map -> map.entrySet().stream())
      .sorted(Comparator.comparing(codeIdEntry -> -codeIdEntry.getKey().length()))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
        (v1, v2) -> v2,
        LinkedHashMap::new));

    return codeIdsMap.entrySet().stream()
      .filter(codeIdEntry -> value.startsWith(codeIdEntry.getKey()))
      .map(Map.Entry::getValue)
      .findFirst()
      .orElse(null);
  }

  private static String removeWhitespaces(String str) {
    return str == null ? null : str.replaceAll("\\s+", "");
  }

}
