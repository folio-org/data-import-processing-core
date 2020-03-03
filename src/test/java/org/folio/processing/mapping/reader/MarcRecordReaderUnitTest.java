package org.folio.processing.mapping.reader;

import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.MarcAuthorityReaderFactory;
import org.folio.processing.mapping.mapper.reader.record.MarcBibReaderFactory;
import org.folio.processing.mapping.mapper.reader.record.MarcHoldingsReaderFactory;
import org.folio.processing.value.Value;
import org.folio.processing.value.Value.ValueType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.HashMap;

import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.EntityType.MARC_HOLDINGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnit4.class)
public class MarcRecordReaderUnitTest {
  private final String RECORD = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"009221\"}, { \"245\":\"American Bar Association journal\" } ] }";

  /*  Reading existing field from MARC record  */
  @Test
  public void shouldRead_IndexTitle_FromBibliographic() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), RECORD);
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload);
    // when
    Value indexTitle = reader.read("245");
    // then
    assertNotNull(indexTitle);
    assertEquals(ValueType.STRING, indexTitle.getType());
    assertEquals("American Bar Association journal", indexTitle.getValue());
  }

  @Test
  public void shouldRead_IndexTitle_FromHoldings() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), RECORD);
    eventPayload.setContext(context);
    Reader reader = new MarcHoldingsReaderFactory().createReader();
    reader.initialize(eventPayload);
    // when
    Value indexTitle = reader.read("245");
    // then
    assertNotNull(indexTitle);
    assertEquals(ValueType.STRING, indexTitle.getType());
    assertEquals("American Bar Association journal", indexTitle.getValue());
  }

  @Test
  public void shouldRead_IndexTitle_FromAuthority() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(), RECORD);
    eventPayload.setContext(context);
    Reader reader = new MarcAuthorityReaderFactory().createReader();
    reader.initialize(eventPayload);
    // when
    Value indexTitle = reader.read("245");
    // then
    assertNotNull(indexTitle);
    assertEquals(ValueType.STRING, indexTitle.getType());
    assertEquals("American Bar Association journal", indexTitle.getValue());
  }

  /*  Reading missing field from MARC record  */
  @Test
  public void shouldRead_MissingField_FromBibliographic() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), RECORD);
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload);
    // when
    Value missingField = reader.read("999");
    // then
    assertNotNull(missingField);
    assertEquals(ValueType.MISSING, missingField.getType());
  }

  @Test
  public void shouldRead_MissingField_FromHoldings() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), RECORD);
    eventPayload.setContext(context);
    Reader reader = new MarcHoldingsReaderFactory().createReader();
    reader.initialize(eventPayload);
    // when
    Value missingField = reader.read("999");
    // then
    assertNotNull(missingField);
    assertEquals(ValueType.MISSING, missingField.getType());
  }

  @Test
  public void shouldRead_MissingField_FromAuthority() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(), RECORD);
    eventPayload.setContext(context);
    Reader reader = new MarcAuthorityReaderFactory().createReader();
    reader.initialize(eventPayload);
    // when
    Value missingField = reader.read("999");
    // then
    assertNotNull(missingField);
    assertEquals(ValueType.MISSING, missingField.getType());
  }

  /* Reading null field from MARC record */
  @Test(expected = NullPointerException.class)
  public void shouldThrowException_OnRead_NullField_FromBibliographic() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), RECORD);
    eventPayload.setContext(context);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventPayload);
    // when
    reader.read(null);
    // then expect NullPointerException
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowException_OnRead_NullField_FromHoldings() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), RECORD);
    eventPayload.setContext(context);
    Reader reader = new MarcHoldingsReaderFactory().createReader();
    reader.initialize(eventPayload);
    // when
    reader.read(null);
    // then expect NullPointerException
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowException_OnRead_NullField_FromAuthority() throws IOException {
    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(), RECORD);
    eventPayload.setContext(context);
    Reader reader = new MarcAuthorityReaderFactory().createReader();
    reader.initialize(eventPayload);
    // when
    reader.read(null);
    // then expect NullPointerException
  }
}
