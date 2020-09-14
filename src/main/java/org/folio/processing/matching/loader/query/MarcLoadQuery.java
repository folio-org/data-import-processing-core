package org.folio.processing.matching.loader.query;

public class MarcLoadQuery implements LoadQuery {

  private final String whereClause;

  public MarcLoadQuery(String whereClause) {
    this.whereClause = whereClause;
  }

  @Override
  public String getSql() {
    return String.format("SELECT records_lb.id, records_lb.snapshotid, records_lb.matchedid, records_lb.generation, records_lb.recordtype, \n" +
      "raw_records_lb.content AS rawRecord, marc_records_lb.content AS parsedRecord, error_records_lb.content AS errorRecord, \n" +
      "records_lb.instanceid AS instanceId, records_lb.suppressdiscovery AS suppressDiscovery, records_lb.state AS state\n" +
      "FROM diku_mod_source_record_storage.records_lb \n" +
      "LEFT OUTER JOIN diku_mod_source_record_storage.marc_records_lb ON records_lb.id = marc_records_lb.id\n" +
      "LEFT OUTER JOIN diku_mod_source_record_storage.raw_records_lb ON records_lb.id = raw_records_lb.id\n" +
      "LEFT OUTER JOIN diku_mod_source_record_storage.error_records_lb ON records_lb.id = error_records_lb.id\n" +
      "%s;", whereClause);
  }

  @Override
  public String getCql() {
    throw new UnsupportedOperationException("CQl query is not applicable for non-jsonb fields");
  }
}
