package org.folio.processing.mapping.writer;

import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.processing.value.BooleanValue;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.RepeatableFieldValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class JsonBasedWriterUnitTest {
  private static final JsonBasedWriter WRITER = new JsonBasedWriter(EntityType.INSTANCE);

  @Test
  public void shouldWrite_Values() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("indexTitle", StringValue.of("The Journal of ecclesiastical history."));
    WRITER.write("classification.number", StringValue.of("N7326 .T12 1991"));
    WRITER.write("languages[]", ListValue.of(asList("eng", "lat")));
    WRITER.write("contributor.names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    WRITER.write("contributor.active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"" +
      "indexTitle\":\"The Journal of ecclesiastical history.\",\"" +
      "classification\":{\"number\":\"N7326 .T12 1991\"},\"" +
      "languages\":[\"eng\",\"lat\"],\"" +
      "contributor\":{\"" +
      "names\":[\"Heins\",\"Rattu\",\"Tabrani\"]," +
      "\"active\":false" +
      "}" +
      "}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldWrite_RepeatableDeleteValues() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"contributor\":[{\"active\":true,\"id\":\"UUID2\",\"names\":[\"1\",\"2\",\"3\"]}]}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("contributor[].id", StringValue.of("UUID"));
    object1.put("contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("contributor[].names[]", ListValue.of(asList("1", "2", "3")));
    object2.put("contributor[].id", StringValue.of("UUID2"));
    object2.put("contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, MappingRule.RepeatableFieldAction.DELETE_EXISTING, "contributor");
    WRITER.write("contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{}", resultInstance);
  }

  @Test
  public void shouldWrite_RepeatableDeleteIncomingValues() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"contributor\":[{\"active\":true,\"id\":\"UUID2\",\"names\":[\"1\",\"2\",\"3\"]}]}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("contributor[].id", StringValue.of("UUID"));
    object1.put("contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("contributor[].names[]", ListValue.of(asList("1", "2", "3")));
    object2.put("contributor[].id", StringValue.of("UUID2"));
    object2.put("contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, MappingRule.RepeatableFieldAction.DELETE_INCOMING, "contributor");
    WRITER.write("contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{\"contributor\":[]}", resultInstance);
  }

  @Test
  public void shouldWrite_RepeatableExchangeValues() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"contributor\":[{\"active\":true,\"id\":\"UUID4\",\"names\":[\"Tabrani\"]},{\"active\":false,\"id\":\"UUID3\",\"names\":[\"3\"]}]}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("contributor[].id", StringValue.of("UUID"));
    object1.put("contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("contributor[].names[]", ListValue.of(asList("1", "2", "3")));
    object2.put("contributor[].id", StringValue.of("UUID2"));
    object2.put("contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, MappingRule.RepeatableFieldAction.EXCHANGE_EXISTING, "contributor");
    WRITER.write("contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"contributor\":[{\"active\":true,\"id\":\"UUID4\",\"names\":[\"Tabrani\"]},{\"active\":false,\"id\":\"UUID3\",\"names\":[\"3\"]},{\"active\":false,\"id\":\"UUID\",\"names\":[\"Heins\",\"Rattu\",\"Tabrani\"]},{\"active\":true,\"id\":\"UUID2\",\"names\":[\"1\",\"2\",\"3\"]}]}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldWrite_RepeatableExtendValues() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"contributor\":[{\"active\":true,\"id\":\"UUID4\",\"names\":[\"Tabrani\"]},{\"active\":false,\"id\":\"UUID3\",\"names\":[\"3\"]}]}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("contributor[].id", StringValue.of("UUID"));
    object1.put("contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("contributor[].names[]", ListValue.of(asList("1", "2", "3")));
    object2.put("contributor[].id", StringValue.of("UUID2"));
    object2.put("contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, MappingRule.RepeatableFieldAction.EXTEND_EXISTING, "contributor");
    WRITER.write("contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"contributor\":[{\"active\":true,\"id\":\"UUID4\",\"names\":[\"Tabrani\"]},{\"active\":false,\"id\":\"UUID3\",\"names\":[\"3\"]},{\"active\":false,\"id\":\"UUID\",\"names\":[\"Heins\",\"Rattu\",\"Tabrani\"]},{\"active\":true,\"id\":\"UUID2\",\"names\":[\"1\",\"2\",\"3\"]}]}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }


  @Test
  public void shouldOverride_StringValue() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("indexTitle", StringValue.of("The Journal of ecclesiastical history."));
    WRITER.write("indexTitle", StringValue.of("Folk music West"));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"indexTitle\":\"Folk music West\"}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldAddValues_OnArrayOverride() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("languages[]", ListValue.of(asList("eng", "lat")));
    WRITER.write("languages[]", ListValue.of(asList("ger", "ita")));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"languages\":[\"eng\",\"lat\",\"ger\",\"ita\"]}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailOnEmptyFieldPath() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("", StringValue.of("The Journal of ecclesiastical history."));
    WRITER.getResult(eventContext);
    // then expect IllegalStateException
  }

  @Test(expected = IllegalStateException.class)
  public void shouldFailIfArrayIsNotLast() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("contributors[].name", StringValue.of("Ernst"));
    WRITER.getResult(eventContext);
    // then expect IllegalStateException
  }

  @Test(expected = IllegalStateException.class)
  public void shouldFailOnWrite_ListValue_To_ObjectField() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("indexTitle", ListValue.of(Arrays.asList("The Journal of ecclesiastical history.")));
    WRITER.getResult(eventContext);
    // then expect IllegalStateException
  }

  @Test(expected = IllegalStateException.class)
  public void shouldFailOnWrite_StringValue_To_ArrayField() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("languages[]", StringValue.of("eng"));
    WRITER.getResult(eventContext);
    // then expect IllegalStateException
  }
}
