package org.folio.processing.mapping.reader;

import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.MarcAuthorityReaderFactory;
import org.folio.processing.mapping.mapper.reader.record.MarcBibReaderFactory;
import org.folio.processing.mapping.mapper.value.Value;
import org.folio.processing.mapping.mapper.value.Value.ValueType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import scala.Predef;

import java.io.IOException;

import static org.folio.processing.mapping.model.MappingProfile.EntityType.MARC_AUTHORITY;
import static org.folio.processing.mapping.model.MappingProfile.EntityType.MARC_BIBLIOGRAPHIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnit4.class)
public class RecordReaderUnitTest {
  private final String RECORD = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"009221\"}, { \"245\":\"American Bar Association journal\" } ] }";

  @Test
  public void shouldRead_IndexTitle_FromBib() throws IOException {
    // given
    EventContext eventContext = new EventContext();
    eventContext.putObject(MARC_BIBLIOGRAPHIC.value(), RECORD);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventContext);
    // when
    Value indexTitle = reader.read("245");
    // then
    assertNotNull(indexTitle);
    assertEquals(ValueType.STRING, indexTitle.getType());
    assertEquals("American Bar Association journal", indexTitle.getValue());
  }

  @Test
  public void shouldRead_MissingField_FromBib() throws IOException {
    // given
    EventContext eventContext = new EventContext();
    eventContext.putObject(MARC_BIBLIOGRAPHIC.value(), RECORD);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventContext);
    // when
    Value missingField = reader.read("999");
    // then
    assertNotNull(missingField);
    assertEquals(ValueType.MISSING, missingField.getType());
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowException_OnRead_NullField_FromBib() throws IOException {
    // given
    EventContext eventContext = new EventContext();
    eventContext.putObject(MARC_BIBLIOGRAPHIC.value(), RECORD);
    Reader reader = new MarcBibReaderFactory().createReader();
    reader.initialize(eventContext);
    // when
    Value nullField = reader.read(null);
    // then expect NullPointerException
  }
}
