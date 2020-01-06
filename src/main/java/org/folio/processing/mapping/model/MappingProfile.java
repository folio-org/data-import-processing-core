package org.folio.processing.mapping.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MappingProfile {
  private EntityType incomingRecordType;
  private EntityType existingRecordType;
  private List<Rule> mappingRules = new ArrayList<>();

  public MappingProfile(EntityType incomingRecordType, EntityType existingRecordType) {
    this.incomingRecordType = incomingRecordType;
    this.existingRecordType = existingRecordType;
  }

  public EntityType getIncomingRecordType() {
    return incomingRecordType;
  }

  public void setIncomingRecordType(EntityType incomingRecordType) {
    this.incomingRecordType = incomingRecordType;
  }

  public EntityType getExistingRecordType() {
    return existingRecordType;
  }

  public void setExistingRecordType(EntityType existingRecordType) {
    this.existingRecordType = existingRecordType;
  }

  public List<Rule> getMappingRules() {
    return mappingRules;
  }

  public void setMappingRules(List<Rule> mappingRules) {
    this.mappingRules = mappingRules;
  }

  public enum EntityType {

    INSTANCE("INSTANCE"),
    HOLDINGS("HOLDINGS"),
    ITEM("ITEM"),
    ORDER("ORDER"),
    INVOICE("INVOICE"),
    MARC_BIBLIOGRAPHIC("MARC_BIBLIOGRAPHIC"),
    MARC_HOLDINGS("MARC_HOLDINGS"),
    MARC_AUTHORITY("MARC_AUTHORITY"),
    EDIFACT_INVOICE("EDIFACT_INVOICE"),
    DELIMITED("DELIMITED");
    private static final Map<String, EntityType> CONSTANTS = new HashMap<>();

    static {
      for (EntityType c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }

    private final String value;

    EntityType(String value) {
      this.value = value;
    }

    public static EntityType fromValue(String value) {
      EntityType constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }

    @Override
    public String toString() {
      return this.value;
    }

    public String value() {
      return this.value;
    }

  }
}
