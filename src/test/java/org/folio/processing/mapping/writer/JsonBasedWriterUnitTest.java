package org.folio.processing.mapping.writer;

import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.processing.mapping.model.MappingProfile;

import org.folio.processing.value.ListValue;
import org.folio.processing.value.StringValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import static java.util.Arrays.*;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class JsonBasedWriterUnitTest {
  private static final JsonBasedWriter WRITER = new JsonBasedWriter(MappingProfile.EntityType.INSTANCE);

  @Test
  public void shouldWrite_Values() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MappingProfile.EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("indexTitle", StringValue.of("The Journal of ecclesiastical history."));
    WRITER.write("classification.number", StringValue.of("N7326 .T12 1991"));
    WRITER.write("languages[]", ListValue.of(asList("eng", "lat")));
    WRITER.write("contributor.names[]", ListValue.of(asList("Heins", "Rattu", "Tabrani")));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"" +
      "indexTitle\":\"The Journal of ecclesiastical history.\",\"" +
      "classification\":{\"number\":\"N7326 .T12 1991\"},\"" +
      "languages\":[\"eng\",\"lat\"],\"" +
      "contributor\":{\"" +
          "names\":[\"Heins\",\"Rattu\",\"Tabrani\"]" +
        "}" +
      "}";
    String resultInstance = eventContext.getContext().get(MappingProfile.EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldOverride_StringValue() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MappingProfile.EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("indexTitle", StringValue.of("The Journal of ecclesiastical history."));
    WRITER.write("indexTitle", StringValue.of("Folk music West"));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"indexTitle\":\"Folk music West\"}";
    String resultInstance = eventContext.getContext().get(MappingProfile.EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test
  public void shouldAddValues_OnArrayOverride() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MappingProfile.EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("languages[]", ListValue.of(asList("eng", "lat")));
    WRITER.write("languages[]", ListValue.of(asList("ger", "ita")));
    WRITER.getResult(eventContext);
    // then
    String expectedInstance = "{\"languages\":[\"eng\",\"lat\",\"ger\",\"ita\"]}";
    String resultInstance = eventContext.getContext().get(MappingProfile.EntityType.INSTANCE.value());
    assertEquals(expectedInstance, resultInstance);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailOnEmptyFieldPath() throws IOException {
    // given
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MappingProfile.EntityType.INSTANCE.value(), "{}");
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
    context.put(MappingProfile.EntityType.INSTANCE.value(), "{}");
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
    context.put(MappingProfile.EntityType.INSTANCE.value(), "{}");
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
    context.put(MappingProfile.EntityType.INSTANCE.value(), "{}");
    eventContext.setContext(context);
    // when
    WRITER.initialize(eventContext);
    WRITER.write("languages[]", StringValue.of("eng"));
    WRITER.getResult(eventContext);
    // then expect IllegalStateException
  }
}
