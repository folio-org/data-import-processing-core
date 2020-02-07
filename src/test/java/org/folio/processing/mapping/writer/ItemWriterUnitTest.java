package org.folio.processing.mapping.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.CirculationNote_;
import org.folio.Item;
import org.folio.MaterialType;
import org.folio.PermanentLoanType;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.mapper.FactoryRegistry;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.item.ItemWriterFactory;
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

import static org.folio.processing.mapping.model.MappingProfile.EntityType.ITEM;
import static org.hamcrest.Matchers.is;

@RunWith(JUnit4.class)
public class ItemWriterUnitTest {

  private static FactoryRegistry factoryRegistry;

  @BeforeClass
  public static void setUpClass() {
    factoryRegistry = new FactoryRegistry();
    factoryRegistry.getWriterFactories().add(new ItemWriterFactory());
  }

  private Item item = new Item()
    .withId(UUID.randomUUID().toString())
    .withStatisticalCodeIds(new HashSet<>(Arrays.asList("b5968c9e-cddc-4576-99e3-8e60aed8b0dd", "69640328-788e-43fc-9c3c-af39e243f3b7")))
    .withMaterialType(new MaterialType()
      .withId("1a54b431-2e4f-452d-9cae-9cee66c9a892")
      .withName("book"))
    .withCopyNumbers(new HashSet<>(Arrays.asList("c4a15834-0184-4a6f-9c0c-0ca5bad8286d", "69640328-788e-43fc-9c3c-af39e243f3b7")))
    .withPermanentLoanType(new PermanentLoanType()
      .withId("2b94c631-fca9-4892-a730-03ee529ffe27")
      .withName("Can circulate"))
    .withCirculationNotes(Arrays.asList(
      new CirculationNote_()
        .withNoteType(CirculationNote_.NoteType.CHECK_IN)
        .withNote("test note")
        .withStaffOnly(false),
      new CirculationNote_()
        .withNoteType(CirculationNote_.NoteType.CHECK_IN)
        .withNote("just note")
        .withStaffOnly(false)
    ));

  @Test
  public void shouldWriteHrid() throws IOException {
    // given
    EventContext eventContext = new EventContext();
    eventContext.putObject(ITEM.value(), "{}");
    Writer itemWriter = factoryRegistry.createWriter(ITEM);

    // when
    itemWriter.initialize(eventContext);
    itemWriter.write("hrid", StringValue.of(item.getHrid()));
    itemWriter.getResult(eventContext);

    // then
    String itemJson = eventContext.getObjects().get(ITEM.value());
    Item actualItem = new ObjectMapper().readValue(itemJson, Item.class);
    Assert.assertThat(actualItem.getHrid(), is(item.getHrid()));
  }

  @Test
  public void shouldWriteStatisticalCodeIds() throws IOException {
    // given
    EventContext eventContext = new EventContext();
    eventContext.putObject(ITEM.value(), "{}");
    Writer itemWriter = factoryRegistry.createWriter(ITEM);

    // when
    itemWriter.initialize(eventContext);
    itemWriter.write("statisticalCodeIds[]", ListValue.of(new ArrayList<>(item.getStatisticalCodeIds())));
    itemWriter.getResult(eventContext);

    // then
    String itemJson = eventContext.getObjects().get(ITEM.value());
    Item actualItem = new ObjectMapper().readValue(itemJson, Item.class);
    Assert.assertTrue(actualItem.getStatisticalCodeIds().containsAll(item.getStatisticalCodeIds()));
  }

  @Test
  public void shouldWriteCirculationNotes() throws IOException {
    // given
    EventContext eventContext = new EventContext();
    eventContext.putObject(ITEM.value(), "{}");
    Writer itemWriter = factoryRegistry.createWriter(ITEM);

    // when
    itemWriter.initialize(eventContext);
    for (CirculationNote_ note : item.getCirculationNotes()) {
      MapValue value = new MapValue()
        .putEntry("noteType", note.getNoteType().value())
        .putEntry("note", note.getNote())
        .putEntry("staffOnly", note.getStaffOnly().toString());
      itemWriter.write("circulationNotes[]", value);
    }
    itemWriter.getResult(eventContext);

    // then
    String itemJson = eventContext.getObjects().get(ITEM.value());
    Item actualItem = new ObjectMapper().readValue(itemJson, Item.class);
    Assert.assertTrue(actualItem.getCirculationNotes().containsAll(item.getCirculationNotes()));
  }

  @Test
  public void shouldWritePermanentLoanType() throws IOException {
    // given
    EventContext eventContext = new EventContext();
    eventContext.putObject(ITEM.value(), "{}");
    Writer itemWriter = factoryRegistry.createWriter(ITEM);

    // when
    itemWriter.initialize(eventContext);
    MapValue value = new MapValue()
      .putEntry("id", item.getPermanentLoanType().getId())
      .putEntry("name", item.getPermanentLoanType().getName());
    itemWriter.write("permanentLoanType", value);
    itemWriter.getResult(eventContext);

    // then
    String itemJson = eventContext.getObjects().get(ITEM.value());
    Item actualItem = new ObjectMapper().readValue(itemJson, Item.class);
    Assert.assertThat(actualItem.getPermanentLoanType(), is(item.getPermanentLoanType()));
  }
}
