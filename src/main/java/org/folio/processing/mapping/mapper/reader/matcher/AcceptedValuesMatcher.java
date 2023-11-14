package org.folio.processing.mapping.mapper.reader.matcher;

public interface AcceptedValuesMatcher {

  boolean matches(String acceptedValue, String valueToCompare);

}
