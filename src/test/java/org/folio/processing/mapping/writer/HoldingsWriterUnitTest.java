package org.folio.processing.mapping.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.ElectronicAccess;
import org.folio.Entry;
import org.folio.Holdingsrecord;
import org.folio.ReceivingHistory;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.mapper.FactoryRegistry;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.holdings.HoldingsWriterFactory;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MapValue;
import org.folio.processing.value.StringValue;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

import static org.folio.processing.mapping.model.MappingProfile.EntityType.HOLDINGS;
import static org.hamcrest.Matchers.is;

@RunWith(JUnit4.class)
public class HoldingsWriterUnitTest {

  private static FactoryRegistry factoryRegistry;

  private Holdingsrecord holdingsRecord = new Holdingsrecord()
    .withId(UUID.randomUUID().toString())
    .withHrid("1024")
    .withFormerIds(new HashSet<>(Arrays.asList("c4a15834-0184-4a6f-9c0c-0ca5bad8286d", "69640328-788e-43fc-9c3c-af39e243f3b7")))
    .withElectronicAccess(Arrays.asList(
      new ElectronicAccess()
        .withUri("https://search.proquest.com/publication/1396348")
        .withPublicNote("via ProQuest, the last 12 months are not available due to an embargo")
        .withRelationshipId("f5d0068e-6272-458e-8a81-b85e7b9a14aa")
        .withMaterialsSpecification("1.2012 -"),
      new ElectronicAccess()
        .withUri("https://www.emeraldinsight.com/loi/jepp")
        .withPublicNote("via Emerald")
        .withRelationshipId("f5d0068e-6272-458e-8a81-b85e7b9a14aa")
        .withMaterialsSpecification("1.2012 - 5.2016")))
    .withReceivingHistory(new ReceivingHistory()
      .withDisplayType("test type")
      .withEntries(Arrays.asList(
        new Entry()
          .withPublicDisplay(true)
          .withEnumeration("v.71:no.6-2")
          .withChronology("no.6-2"),
        new Entry()
          .withPublicDisplay(true)
          .withEnumeration("v.71:no.6-3")
          .withChronology("no.6-3"))));

  @BeforeClass
  public static void setUpClass() {
    factoryRegistry = new FactoryRegistry();
    factoryRegistry.getWriterFactories().add(new HoldingsWriterFactory());
  }

  @Test
  public void shouldWriteHrid() throws IOException {
    // given
    EventContext eventContext = new EventContext();
    eventContext.putObject(HOLDINGS.value(), "{}");
    Writer holdingsWriter = factoryRegistry.createWriter(HOLDINGS);

    // when
    holdingsWriter.initialize(eventContext);
    holdingsWriter.write("hrid", StringValue.of(holdingsRecord.getHrid()));
    holdingsWriter.getResult(eventContext);

    // then
    String holdingsRecordJson = eventContext.getObjects().get(HOLDINGS.value());
    Holdingsrecord actualHoldingsRecord = new ObjectMapper().readValue(holdingsRecordJson, Holdingsrecord.class);
    Assert.assertThat(actualHoldingsRecord.getHrid(), is(holdingsRecord.getHrid()));
  }

  @Test
  public void shouldWriteFormerIds() throws IOException {
    // given
    EventContext eventContext = new EventContext();
    eventContext.putObject(HOLDINGS.value(), "{}");
    Writer holdingsWriter = factoryRegistry.createWriter(HOLDINGS);

    // when
    holdingsWriter.initialize(eventContext);
    holdingsWriter.write("formerIds[]", ListValue.of(new ArrayList<>(holdingsRecord.getFormerIds())));
    holdingsWriter.getResult(eventContext);

    // then
    String holdingsRecordJson = eventContext.getObjects().get(HOLDINGS.value());
    Holdingsrecord actualHoldingsRecord = new ObjectMapper().readValue(holdingsRecordJson, Holdingsrecord.class);
    Assert.assertTrue(actualHoldingsRecord.getFormerIds().containsAll(actualHoldingsRecord.getFormerIds()));
  }

  @Test
  public void shouldWriteElectronicAccess() throws IOException {
    // given
    EventContext eventContext = new EventContext();
    eventContext.putObject(HOLDINGS.value(), "{}");
    Writer holdingsWriter = factoryRegistry.createWriter(HOLDINGS);

    // when
    holdingsWriter.initialize(eventContext);
    for (ElectronicAccess electronicAccess : holdingsRecord.getElectronicAccess()) {
      MapValue value = new MapValue()
        .putEntry("uri", electronicAccess.getUri())
        .putEntry("materialsSpecification", electronicAccess.getMaterialsSpecification())
        .putEntry("publicNote", electronicAccess.getPublicNote())
        .putEntry("relationshipId", electronicAccess.getRelationshipId());
      holdingsWriter.write("electronicAccess[]", value);
    }
    holdingsWriter.getResult(eventContext);

    // then
    String holdingsRecordJson = eventContext.getObjects().get(HOLDINGS.value());
    Holdingsrecord actualHoldingsRecord = new ObjectMapper().readValue(holdingsRecordJson, Holdingsrecord.class);
    Assert.assertTrue(actualHoldingsRecord.getElectronicAccess().containsAll(holdingsRecord.getElectronicAccess()));
  }

  @Test
  public void shouldWriteReceivingHistory() throws IOException {
    // given
    FactoryRegistry factoryRegistry = new FactoryRegistry();
    factoryRegistry.getWriterFactories().add(new HoldingsWriterFactory());
    EventContext eventContext = new EventContext();
    eventContext.putObject(HOLDINGS.value(), "{}");
    Writer holdingsWriter = factoryRegistry.createWriter(HOLDINGS);

    // when
    ReceivingHistory expectedReceivingHistory = holdingsRecord.getReceivingHistory();
    holdingsWriter.initialize(eventContext);
    holdingsWriter.write("receivingHistory.displayType", StringValue.of(expectedReceivingHistory.getDisplayType()));
    for (Entry entry : expectedReceivingHistory.getEntries()) {
      MapValue value = new MapValue()
        .putEntry("publicDisplay", entry.getPublicDisplay().toString())
        .putEntry("enumeration", entry.getEnumeration())
        .putEntry("chronology", entry.getChronology());
      holdingsWriter.write("receivingHistory.entries[]", value);
    }
    holdingsWriter.getResult(eventContext);

    // then
    String holdingsRecordJson = eventContext.getObjects().get(HOLDINGS.value());
    Holdingsrecord actualHoldingsRecord = new ObjectMapper().readValue(holdingsRecordJson, Holdingsrecord.class);
    Assert.assertThat(actualHoldingsRecord.getReceivingHistory().getDisplayType(), is(expectedReceivingHistory.getDisplayType()));
    Assert.assertTrue(actualHoldingsRecord.getReceivingHistory().getEntries().containsAll(expectedReceivingHistory.getEntries()));
  }
}
