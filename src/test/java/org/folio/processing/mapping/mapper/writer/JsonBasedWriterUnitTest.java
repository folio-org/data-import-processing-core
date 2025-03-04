package org.folio.processing.mapping.mapper.writer;

import com.google.common.collect.Lists;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.folio.rest.jaxrs.model.MappingRule.BooleanFieldAction.ALL_FALSE;
import static org.folio.rest.jaxrs.model.MappingRule.BooleanFieldAction.ALL_TRUE;
import static org.folio.rest.jaxrs.model.MappingRule.RepeatableFieldAction.DELETE_EXISTING;
import static org.folio.rest.jaxrs.model.MappingRule.RepeatableFieldAction.DELETE_INCOMING;
import static org.folio.rest.jaxrs.model.MappingRule.RepeatableFieldAction.EXCHANGE_EXISTING;
import static org.folio.rest.jaxrs.model.MappingRule.RepeatableFieldAction.EXTEND_EXISTING;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class JsonBasedWriterUnitTest {
  private static final JsonBasedWriter WRITER = new JsonBasedWriter(EntityType.INSTANCE);
  private static final JsonBasedWriter ORDER_WRITER = new JsonBasedWriter(EntityType.ORDER);

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
    context.put(EntityType.INSTANCE.value(), "{\"instance\": {\"contributor\": [{\"active\": true,\"id\": \"UUID2\",\"names\": [\"1\", \"2\", \"3\"]}]}}\n");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("instance.contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("instance.contributor[].id", StringValue.of("UUID"));
    object1.put("instance.contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("instance.contributor[].names[]", ListValue.of(asList("1", "2", "3")));
    object2.put("instance.contributor[].id", StringValue.of("UUID2"));
    object2.put("instance.contributor[].active", BooleanValue.of(ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, MappingRule.RepeatableFieldAction.DELETE_EXISTING, "contributor");
    WRITER.write("instance.contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{\"instance\":{}}", resultInstance);
  }

  @Test
  public void shouldWrite_RepeatableDeleteIncomingValues() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"instance\": {\"contributor\": [{\"active\": true,\"id\": \"UUID2\",\"names\": [\"1\", \"2\", \"3\"]},{\"active\": false,\"id\": \"UUID\",\"names\": [\"Heins\", \"Rattu\", \"Tabrani\"]},{\"active\": true,\"id\": \"UUID1\",\"names\": [ \"2\"]}]}}\n");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("instance.contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("instance.contributor[].id", StringValue.of("UUID"));
    object1.put("instance.contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("instance.contributor[].names[]", ListValue.of(asList("1", "2")));
    object2.put("instance.contributor[].id", StringValue.of("UUID3"));
    object2.put("instance.contributor[].active", BooleanValue.of(ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, MappingRule.RepeatableFieldAction.DELETE_INCOMING, "contributor");
    WRITER.write("instance.contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{\"instance\":{\"contributor\":[{\"active\":true,\"id\":\"UUID2\",\"names\":[\"1\",\"2\",\"3\"]},{\"active\":true,\"id\":\"UUID1\",\"names\":[\"2\"]}]}}", resultInstance);
  }

  @Test
  public void shouldWrite_RepeatableDeleteIncomingValuesIfThereAreSomeAdditionalFieldsExistsInEntity() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"instance\": {\"contributor\": [{\"active\": true,\"id\": \"UUID2\",\"names\": [\"1\", \"2\", \"3\"]},{\"active\": false,\"id\": \"UUID\",\"names\": [\"Heins\", \"Rattu\", \"Tabrani\"]},{\"active\": true,\"id\": \"UUID1\",\"names\": [ \"2\"]}]}}\n");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("instance.contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("instance.contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("instance.contributor[].names[]", ListValue.of(asList("1", "2")));
    object2.put("instance.contributor[].active", BooleanValue.of(ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, MappingRule.RepeatableFieldAction.DELETE_INCOMING, "contributor");
    WRITER.write("instance.contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{\"instance\":{\"contributor\":[{\"active\":true,\"id\":\"UUID2\",\"names\":[\"1\",\"2\",\"3\"]},{\"active\":true,\"id\":\"UUID1\",\"names\":[\"2\"]}]}}", resultInstance);
  }

  @Test
  public void shouldWrite_RepeatableExchangeValues() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"instance\":{\"contributor\":[{\"active\":true,\"id\":\"UUID4\",\"names\":[\"Tabrani\"]},{\"active\":false,\"id\":\"UUID3\",\"names\":[\"3\"]}]}}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("instance.contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("instance.contributor[].id", StringValue.of("UUID"));
    object1.put("instance.contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("instance.contributor[].names[]", ListValue.of(asList("1", "2", "3")));
    object2.put("instance.contributor[].id", StringValue.of("UUID2"));
    object2.put("instance.contributor[].active", BooleanValue.of(ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, MappingRule.RepeatableFieldAction.EXCHANGE_EXISTING, "contributor");
    WRITER.write("instance.contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"instance\":{\"contributor\":[{\"active\":false,\"names\":[\"Heins\",\"Rattu\",\"Tabrani\"],\"id\":\"UUID\"},{\"active\":true,\"names\":[\"1\",\"2\",\"3\"],\"id\":\"UUID2\"}]}}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldWrite_RepeatableExtendValues() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"instance\": {\"contributor\":[{\"active\":true,\"id\":\"UUID4\",\"names\":[\"Tabrani\"]},{\"active\":false,\"id\":\"UUID3\",\"names\":[\"3\"]}]}}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("instance.contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("instance.contributor[].id", StringValue.of("UUID"));
    object1.put("instance.contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("instance.contributor[].names[]", ListValue.of(asList("1", "2", "3")));
    object2.put("instance.contributor[].id", StringValue.of("UUID2"));
    object2.put("instance.contributor[].active", BooleanValue.of(ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, EXTEND_EXISTING, "contributor");
    WRITER.write("instance.contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"instance\":{\"contributor\":[{\"active\":true,\"id\":\"UUID4\",\"names\":[\"Tabrani\"]},{\"active\":false,\"id\":\"UUID3\",\"names\":[\"3\"]},{\"active\":false,\"names\":[\"Heins\",\"Rattu\",\"Tabrani\"],\"id\":\"UUID\"},{\"active\":true,\"names\":[\"1\",\"2\",\"3\"],\"id\":\"UUID2\"}]}}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldWrite_RepeatableDeleteValuesIfSubfieldsAreEmpty() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"instance\":{\"contributor\":[{\"active\":true,\"id\":\"UUID2\",\"names\":[\"1\",\"2\",\"3\"]}]}}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);

    RepeatableFieldValue field = RepeatableFieldValue.of(Collections.emptyList(), MappingRule.RepeatableFieldAction.DELETE_EXISTING, "contributor");
    WRITER.write("instance.contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{\"instance\":{}}", resultInstance);
  }

  @Test
  public void shouldNotChangeContext_RepeatableExtendValuesIfEntityIsEmpty() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("instance.contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("instance.contributor[].id", StringValue.of("UUID"));
    object1.put("instance.contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("instance.contributor[].names[]", ListValue.of(asList("1", "2", "3")));
    object2.put("instance.contributor[].id", StringValue.of("UUID2"));
    object2.put("instance.contributor[].active", BooleanValue.of(ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, EXTEND_EXISTING, "contributor");
    WRITER.write("instance.contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"instance\":{\"contributor\":[{\"active\":false,\"names\":[\"Heins\",\"Rattu\",\"Tabrani\"],\"id\":\"UUID\"},{\"active\":true,\"names\":[\"1\",\"2\",\"3\"],\"id\":\"UUID2\"}]}}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldNotChangeContext_RepeatableDeleteIncomingValuesIfEntityIsEmpty() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("instance.contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("instance.contributor[].id", StringValue.of("UUID"));
    object1.put("instance.contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("instance.contributor[].names[]", ListValue.of(asList("1", "2")));
    object2.put("instance.contributor[].id", StringValue.of("UUID3"));
    object2.put("instance.contributor[].active", BooleanValue.of(ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, MappingRule.RepeatableFieldAction.DELETE_INCOMING, "contributor");
    WRITER.write("instance.contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{}", resultInstance);
  }

  @Test
  public void shouldNotChangeContext_RepeatableExchangeValuesIfEntityIsEmpty() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("instance.contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("instance.contributor[].id", StringValue.of("UUID"));
    object1.put("instance.contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("instance.contributor[].names[]", ListValue.of(asList("1", "2", "3")));
    object2.put("instance.contributor[].id", StringValue.of("UUID2"));
    object2.put("instance.contributor[].active", BooleanValue.of(ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, MappingRule.RepeatableFieldAction.EXCHANGE_EXISTING, "contributor");
    WRITER.write("instance.contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"instance\":{\"contributor\":[{\"active\":true,\"names\":[\"1\",\"2\",\"3\"],\"id\":\"UUID2\"}]}}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldNotChangeContext_RepeatableDeleteValuesIfEntityIsEmpty() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    List<Map<String, Value>> objects = new ArrayList<>();
    Map<String, Value> object1 = new HashMap<>();
    Map<String, Value> object2 = new HashMap<>();
    object1.put("instance.contributor[].names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    object1.put("instance.contributor[].id", StringValue.of("UUID"));
    object1.put("instance.contributor[].active", BooleanValue.of(MappingRule.BooleanFieldAction.ALL_FALSE));

    object2.put("instance.contributor[].names[]", ListValue.of(asList("1", "2", "3")));
    object2.put("instance.contributor[].id", StringValue.of("UUID2"));
    object2.put("instance.contributor[].active", BooleanValue.of(ALL_TRUE));

    objects.add(object1);
    objects.add(object2);

    RepeatableFieldValue field = RepeatableFieldValue.of(objects, MappingRule.RepeatableFieldAction.DELETE_EXISTING, "contributor");
    WRITER.write("instance.contributor[]", field);

    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{}", resultInstance);
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
    WRITER.write("indexTitle", ListValue.of(List.of("The Journal of ecclesiastical history.")));
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

  @Test
  public void shouldWrite_ListExtendValues() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"instance\":{\"natureOfContentTermIds\":[\"UUID1\",\"UUID2\"]}}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("instance.natureOfContentTermIds[]", ListValue.of(asList("UUID3", "UUID4"), EXTEND_EXISTING));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"instance\":{\"natureOfContentTermIds\":[\"UUID1\",\"UUID2\",\"UUID3\",\"UUID4\"]}}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldWrite_ListExchangeValues() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"instance\":{\"natureOfContentTermIds\":[\"UUID1\",\"UUID2\"]}}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("instance.natureOfContentTermIds[]", ListValue.of(asList("UUID3", "UUID4"), EXCHANGE_EXISTING));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"instance\":{\"natureOfContentTermIds\":[\"UUID3\",\"UUID4\"]}}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldWrite_ListDeleteIncomingValues() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"instance\":{\"natureOfContentTermIds\": [\"UUID1\",\"UUID2\",\"UUID3\",\"UUID4\"]}}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("instance.natureOfContentTermIds[]", ListValue.of(Lists.newArrayList("UUID1", "UUID2", "UUID3"), DELETE_INCOMING));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"instance\":{\"natureOfContentTermIds\":[\"UUID4\"]}}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldWrite_ListDeleteIncomingValuesIfNonMatch() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"instance\":{\"natureOfContentTermIds\": [\"UUID1\",\"UUID2\",\"UUID3\",\"UUID4\",\"UUID5\"]}}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("instance.natureOfContentTermIds[]", ListValue.of(Lists.newArrayList("UUID6", "UUID7", "UUID8"), DELETE_INCOMING));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"instance\":{\"natureOfContentTermIds\":[\"UUID1\",\"UUID2\",\"UUID3\",\"UUID4\",\"UUID5\"]}}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldWrite_ListDeleteExistingValues() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"instance\":{\"natureOfContentTermIds\":[\"UUID1\",\"UUID2\"]}}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("instance.natureOfContentTermIds[]", ListValue.of(Collections.singletonList("UUID3"), DELETE_EXISTING));
    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{\"instance\":{}}", resultInstance);
  }

  @Test
  public void shouldWrite_ListExtendValuesIfEntityIsEmpty() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("instance.natureOfContentTermIds[]", ListValue.of(asList("UUID3", "UUID4"), EXTEND_EXISTING));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"instance\":{\"natureOfContentTermIds\":[\"UUID3\",\"UUID4\"]}}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldWrite_ListExtendValuesIfContextIsWithAnotherField() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"instance\":{\"invalidField\":[\"UUID1\",\"UUID2\"]}}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("instance.natureOfContentTermIds[]", ListValue.of(asList("UUID3", "UUID4"), EXTEND_EXISTING));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"instance\":{\"invalidField\":[\"UUID1\",\"UUID2\"],\"natureOfContentTermIds\":[\"UUID3\",\"UUID4\"]}}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldWrite_ListExchangeValuesAsExtendIfEntityIsEmpty() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("instance.natureOfContentTermIds[]", ListValue.of(asList("UUID3", "UUID4"), EXCHANGE_EXISTING));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"instance\":{\"natureOfContentTermIds\":[\"UUID3\",\"UUID4\"]}}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldNotChangeContext_ListDeleteIncomingValuesIfEntityIsEmpty() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("instance.natureOfContentTermIds[]", ListValue.of(Collections.singletonList("UUID2"), DELETE_INCOMING));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{}";
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldNotChangeContext_ListDeleteExistingValuesIfEntityIsEmpty() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("instance.natureOfContentTermIds[]", ListValue.of(Collections.singletonList("UUID3"), DELETE_EXISTING));
    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{}", resultInstance);
  }


  @Test
  public void shouldDeleteOnWrite_StringValue() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"instance\": {\"title\":\"bla\", \"catalogedDate\": \"1970-01-01T00:00:00\"}}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    StringValue value = StringValue.of(StringUtils.EMPTY, true);
    WRITER.write("instance.catalogedDate", value);
    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{\"instance\":{\"title\":\"bla\"}}", resultInstance);
  }

  @Test
  public void shouldDeleteOnWrite_WhenObjectNodePathIsSpecified() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"item\": {\"temporaryLoanType\": {\"id\": \"8d0a5eca-25de-4391-81a9-236eeefdd20b\", \"name\": \"Can circulate\"}, \"hrid\": \"it00000000001\"}}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    StringValue value = StringValue.of(StringUtils.EMPTY, true);
    WRITER.write("item.temporaryLoanType.id", value);
    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{\"item\":{\"hrid\":\"it00000000001\"}}", resultInstance);
  }

  @Test
  public void shouldProcessRemoveOnWriteOption_WhenHasNoSpecifiedField() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), "{\"item\": {\"hrid\": \"it00000000001\"}}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    StringValue value = StringValue.of(StringUtils.EMPTY, true);
    WRITER.write("item.temporaryLoanType.id", value);
    WRITER.getResult(eventContext);
    // then
    String resultInstance = eventContext.getContext().get(EntityType.INSTANCE.value());
    assertEquals("{\"item\":{\"hrid\":\"it00000000001\"}}", resultInstance);
  }

  @Test
  public void shouldWriteNestedRepeatableFieldValue() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INVOICE.value(), "{}");
    eventContext.setContext(context);

    String rootPath = "invoice.adjustments[]";
    String fundDistributionsRootPath = "invoice.adjustments[].fundDistributions[]";
    Map<String, Value> fundDistributionElement1_1 = Map.of(
      "invoice.adjustments[].fundDistributions[].fundId", StringValue.of("b2c0e100-0485-43f2-b161-3c60aac9f711"),
      "invoice.adjustments[].fundDistributions[].code", StringValue.of("USHIST-1.1"));
    Map<String, Value> fundDistributionElement1_2 = Map.of(
      "invoice.adjustments[].fundDistributions[].fundId", StringValue.of("b2c0e100-0485-43f2-b161-3c60aac9f712"),
      "invoice.adjustments[].fundDistributions[].code", StringValue.of("USHIST-1.2"));
    Map<String, Value> fundDistributionElement2_1 = Map.of(
      "invoice.adjustments[].fundDistributions[].fundId", StringValue.of("b2c0e100-0485-43f2-b161-3c60aac9f721"),
      "invoice.adjustments[].fundDistributions[].code", StringValue.of("USHIST-2.1"));
    Map<String, Value> fundDistributionElement2_2 = Map.of(
      "invoice.adjustments[].fundDistributions[].fundId", StringValue.of("b2c0e100-0485-43f2-b161-3c60aac9f722"),
      "invoice.adjustments[].fundDistributions[].code", StringValue.of("USHIST-2.2"));

    Map<String, Value> adjustment1 = Map.of(
      "invoice.adjustments[].description", StringValue.of("description-1"),
      "invoice.adjustments[].exportToAccounting", BooleanValue.of(ALL_TRUE),
      "invoice.adjustments[].fundDistributions[]", RepeatableFieldValue.of(List.of(fundDistributionElement1_1, fundDistributionElement1_2), EXTEND_EXISTING, fundDistributionsRootPath));
    Map<String, Value> adjustment2 = Map.of(
      "invoice.adjustments[].description", StringValue.of("description-2"),
      "invoice.adjustments[].exportToAccounting", BooleanValue.of(ALL_FALSE),
      "invoice.adjustments[].fundDistributions[]", RepeatableFieldValue.of(List.of(fundDistributionElement2_1, fundDistributionElement2_2), EXTEND_EXISTING, fundDistributionsRootPath));

    RepeatableFieldValue value = RepeatableFieldValue.of(List.of(adjustment1, adjustment2), EXTEND_EXISTING, rootPath);

    // when
    JsonBasedWriter writer = new JsonBasedWriter(EntityType.INVOICE);
    writer.initialize(eventContext);
    writer.write(value.getRootPath(), value);
    writer.getResult(eventContext);

    // then
    String expectedInvoiceAsString = "{\"invoice\":{\"adjustments\":[{\"fundDistributions\":[{\"code\":\"USHIST-1.1\",\"fundId\":\"b2c0e100-0485-43f2-b161-3c60aac9f711\"},{\"code\":\"USHIST-1.2\",\"fundId\":\"b2c0e100-0485-43f2-b161-3c60aac9f712\"}],\"exportToAccounting\":true,\"description\":\"description-1\"},{\"fundDistributions\":[{\"code\":\"USHIST-2.1\",\"fundId\":\"b2c0e100-0485-43f2-b161-3c60aac9f721\"},{\"code\":\"USHIST-2.2\",\"fundId\":\"b2c0e100-0485-43f2-b161-3c60aac9f722\"}],\"exportToAccounting\":false,\"description\":\"description-2\"}]}}";
    String resultInvoice = eventContext.getContext().get(EntityType.INVOICE.value());
    assertEquals(new JsonObject(expectedInvoiceAsString), new JsonObject(resultInvoice));
  }

  @Test
  public void shouldWrite_ValuesIntoOrderEntity() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.ORDER.value(), "{}");
    eventContext.setContext(context);
    // when
    ORDER_WRITER.initialize(eventContext);
    ORDER_WRITER.write("order.po.workflowStatus", StringValue.of("Pending"));
    ORDER_WRITER.write("order.po.vendor", StringValue.of("11fb627a-cdf1-11e8-a8d5-f2801f1b9fd1"));
    ORDER_WRITER.write("order.po.orderType", StringValue.of("One-Time"));
    ORDER_WRITER.write("order.po.poNumberPrefix", StringValue.of("db9f5d17-0ca3-4d14-ae49-16b63c8fc083"));
    ORDER_WRITER.write("order.po.acqUnitIds[]", ListValue.of(List.of("0ebb1f7d-983f-3026-8a4c-5318e0ebc041")));
    ORDER_WRITER.write("order.poLine.titleOrPackage", StringValue.of("TestingTitle"));
    ORDER_WRITER.write("order.poLine.acquisitionMethod", StringValue.of("796596c4-62b5-4b64-a2ce-524c747afaa2"));
    ORDER_WRITER.write("order.poLine.orderFormat", StringValue.of("P/E Mix"));
    ORDER_WRITER.write("order.poLine.source", StringValue.of("MARC"));
    ORDER_WRITER.write("order.poLine.cost.currency", StringValue.of("UAH"));
    ORDER_WRITER.getResult(eventContext);
    // then
    String expectedOrder = "{\"order\":{\"po\":{\"workflowStatus\":\"Pending\",\"vendor\":\"11fb627a-cdf1-11e8-a8d5-f2801f1b9fd1\",\"orderType\":\"One-Time\",\"poNumberPrefix\":\"db9f5d17-0ca3-4d14-ae49-16b63c8fc083\",\"acqUnitIds\":[\"0ebb1f7d-983f-3026-8a4c-5318e0ebc041\"]},\"poLine\":{\"titleOrPackage\":\"TestingTitle\",\"acquisitionMethod\":\"796596c4-62b5-4b64-a2ce-524c747afaa2\",\"orderFormat\":\"P/E Mix\",\"source\":\"MARC\",\"cost\":{\"currency\":\"UAH\"}}}}";
    String resultOrder = eventContext.getContext().get(EntityType.ORDER.value());
    assertEquals(expectedOrder, resultOrder);
  }

  @Test
  public void shouldSkipEmptyRepeatableFieldValue() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.ORDER.value(), "{}");
    eventContext.setContext(context);
    // when
    ORDER_WRITER.initialize(eventContext);
    ORDER_WRITER.write("order.po.workflowStatus", StringValue.of("Open"));
    ORDER_WRITER.write("order.po.vendor", StringValue.of("11fb627a-cdf1-11e8-a8d5-f2801f1b9fd1"));
    ORDER_WRITER.write("order.po.orderType", StringValue.of("One-Time"));
    ORDER_WRITER.write("order.po.poNumberPrefix", StringValue.of("db9f5d17-0ca3-4d14-ae49-16b63c8fc083"));
    ORDER_WRITER.write("order.po.acqUnitIds[]", ListValue.of(List.of("0ebb1f7d-983f-3026-8a4c-5318e0ebc041")));
    ORDER_WRITER.write("order.poLine.titleOrPackage", StringValue.of("TestingTitle"));
    ORDER_WRITER.write("order.poLine.acquisitionMethod", StringValue.of("796596c4-62b5-4b64-a2ce-524c747afaa2"));
    ORDER_WRITER.write("order.poLine.orderFormat", StringValue.of("P/E Mix"));
    ORDER_WRITER.write("order.poLine.source", StringValue.of("MARC"));
    ORDER_WRITER.write("order.poLine.donorOrganizationIds[]", RepeatableFieldValue.of(List.of(Map.of()), EXTEND_EXISTING, "order.poLine.donorOrganizationIds[]") );
    ORDER_WRITER.write("order.poLine.cost.currency", StringValue.of("UAH"));
    ORDER_WRITER.getResult(eventContext);
    // then
    String expectedOrder = "{\"order\":{\"po\":{\"workflowStatus\":\"Open\",\"vendor\":\"11fb627a-cdf1-11e8-a8d5-f2801f1b9fd1\",\"orderType\":\"One-Time\",\"poNumberPrefix\":\"db9f5d17-0ca3-4d14-ae49-16b63c8fc083\",\"acqUnitIds\":[\"0ebb1f7d-983f-3026-8a4c-5318e0ebc041\"]},\"poLine\":{\"titleOrPackage\":\"TestingTitle\",\"acquisitionMethod\":\"796596c4-62b5-4b64-a2ce-524c747afaa2\",\"orderFormat\":\"P/E Mix\",\"source\":\"MARC\",\"cost\":{\"currency\":\"UAH\"}}}}";
    String resultOrder = eventContext.getContext().get(EntityType.ORDER.value());
    assertEquals(expectedOrder, resultOrder);
  }
}
