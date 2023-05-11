package org.folio.processing.matching.manager;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.MatchingManager;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.folio.processing.matching.matcher.Matcher.MATCHED_VALUES_NUMBER;
import static org.folio.processing.matching.matcher.Matcher.MATCH_FIELD;
import static org.folio.processing.matching.reader.util.MatchIdProcessorUtil.MAPPING_PARAMS_KEY;
import static org.folio.processing.matching.reader.util.MatchIdProcessorUtil.RELATIONS_KEY;
import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;

@RunWith(VertxUnitRunner.class)
public class MatchingManagerTest {
  private MatchValueLoader instanceValueLoader;

  @Before
  public void beforeTest() {
    MatchValueReaderFactory.clearReaderFactory();
    MatchValueLoaderFactory.clearLoaderFactory();
    instanceValueLoader = new MatchValueLoader() {
      @Override
      public CompletableFuture<LoadResult> loadEntity(LoadQuery loadQuery, DataImportEventPayload eventPayload) {
        CompletableFuture<LoadResult> future = new CompletableFuture<>();
        LoadResult result = new LoadResult();
        result.setValue("Some value");
        result.setEntityType("INSTANCE");
        future.complete(result);
        return future;
      }

      @Override
      public boolean isEligibleForEntityType(EntityType existingRecordType) {
        return existingRecordType == INSTANCE;
      }
    };
  }

  @Test
  public void shouldMatch_MarcBibliographicAndEdifact(TestContext testContext) {
    // given
    MatchValueReaderFactory.register(new TestMatchValueReader());
    MatchValueLoaderFactory.register(new TestMatchValueLoader());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withIncomingRecordType(EDIFACT_INVOICE)
      .withMatchDetails(Collections.singletonList(new MatchDetail()));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
    String givenEdifact = UUID.randomUUID().toString();
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(EDIFACT_INVOICE.value(), givenEdifact);
    eventContext.setContext(context);
    eventContext.setCurrentNode(matchProfileWrapper);
    // when
    CompletableFuture<Boolean> result = MatchingManager.match(eventContext);
    // then
    result.whenComplete((matched, throwable) -> {
      testContext.assertEquals("0", eventContext.getContext().get(MATCHED_VALUES_NUMBER));
      testContext.assertEquals(null, eventContext.getContext().get(MATCH_FIELD));
      testContext.assertNull(throwable);
      testContext.assertTrue(matched);
    });
  }

  @Test
  public void shouldMatch_MarcBibliographicAndPopulateNumberOfMatchedValuesAndMatchFieldForListValue(TestContext testContext) {
    // given
    MatchValueReaderFactory.register(new MatchValueReader() {
      @Override
      public Value read(DataImportEventPayload eventPayload, MatchDetail matchDetail) {
        return ListValue.of(List.of("value1", "value2"));
      }

      @Override
      public boolean isEligibleForEntityType(EntityType incomingRecordType) {
        return incomingRecordType == MARC_BIBLIOGRAPHIC;
      }
    });
    MatchValueLoaderFactory.register(instanceValueLoader);

    MatchProfile matchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE)
      .withMatchDetails(Collections.singletonList(new MatchDetail()
        .withExistingMatchExpression(new MatchExpression().withFields(List.of(new Field().withValue("instance.hrid"))))));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    eventContext.setContext(context);
    eventContext.setCurrentNode(matchProfileWrapper);
    // when
    CompletableFuture<Boolean> result = MatchingManager.match(eventContext);
    // then
    result.whenComplete((matched, throwable) -> {
      testContext.assertEquals("2", eventContext.getContext().get(MATCHED_VALUES_NUMBER));
      testContext.assertEquals("hrid", eventContext.getContext().get(MATCH_FIELD));
      testContext.assertNull(throwable);
      testContext.assertTrue(matched);
    });
  }

  @Test
  public void shouldMatch_MarcBibliographicAndPopulateNumberOfMatchedValuesAndMatchFieldForStringValue(TestContext testContext) {
    // given
    MatchValueReaderFactory.register(new MatchValueReader() {
      @Override
      public Value read(DataImportEventPayload eventPayload, MatchDetail matchDetail) {
        return StringValue.of("value1");
      }

      @Override
      public boolean isEligibleForEntityType(EntityType incomingRecordType) {
        return incomingRecordType == MARC_BIBLIOGRAPHIC;
      }
    });
    MatchValueLoaderFactory.register(instanceValueLoader);

    MatchProfile matchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE)
      .withMatchDetails(Collections.singletonList(new MatchDetail()
        .withExistingMatchExpression(new MatchExpression().withFields(List.of(new Field().withValue("instance.hrid"))))));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(MAPPING_PARAMS_KEY, "{}");
    context.put(RELATIONS_KEY, "{}");
    eventContext.setContext(context);
    eventContext.setCurrentNode(matchProfileWrapper);
    // when
    CompletableFuture<Boolean> result = MatchingManager.match(eventContext);
    // then
    result.whenComplete((matched, throwable) -> {
      testContext.assertEquals("1", eventContext.getContext().get(MATCHED_VALUES_NUMBER));
      testContext.assertEquals("hrid", eventContext.getContext().get(MATCH_FIELD));
      testContext.assertNull(throwable);
      testContext.assertTrue(matched);
    });
  }

  @Test
  public void shouldCompleteExceptionally_ifNoEligibleReader(TestContext testContext) {
    // given
    MatchValueLoaderFactory.register(new TestMatchValueLoader());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withIncomingRecordType(EDIFACT_INVOICE)
      .withMatchDetails(Collections.singletonList(new MatchDetail()));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    DataImportEventPayload eventContext = new DataImportEventPayload();
    eventContext.setCurrentNode(matchProfileWrapper);
    // when
    CompletableFuture<Boolean> result = MatchingManager.match(eventContext);
    // then
    result.whenComplete((matched, throwable) -> {
      testContext.assertNotNull(throwable);
      testContext.assertTrue(throwable instanceof MatchingException);
    });
  }

  @Test
  public void shouldCompleteExceptionally_ifNoEligibleLoader(TestContext testContext) {
    // given
    MatchValueReaderFactory.register(new TestMatchValueReader());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withIncomingRecordType(EDIFACT_INVOICE)
      .withMatchDetails(Collections.singletonList(new MatchDetail()));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    DataImportEventPayload eventContext = new DataImportEventPayload();
    eventContext.setCurrentNode(matchProfileWrapper);

    // when
    CompletableFuture<Boolean> result = MatchingManager.match(eventContext);
    // then
    result.whenComplete((matched, throwable) -> {
      testContext.assertNotNull(throwable);
      testContext.assertTrue(throwable instanceof MatchingException);
    });
  }

  @Test
  public void shouldNotMatchIfWrongContentType(TestContext testContext) {
    // given
    MatchValueReaderFactory.register(new TestMatchValueReader());
    MatchValueLoaderFactory.register(new TestMatchValueLoader());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withIncomingRecordType(EDIFACT_INVOICE)
      .withMatchDetails(Collections.singletonList(new MatchDetail()));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);

    String givenMarcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
    String givenEdifact = UUID.randomUUID().toString();
    DataImportEventPayload eventContext = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), givenMarcRecord);
    context.put(EDIFACT_INVOICE.value(), givenEdifact);
    eventContext.setContext(context);
    eventContext.setCurrentNode(matchProfileWrapper);
    // when
    CompletableFuture<Boolean> result = MatchingManager.match(eventContext);
    // then
    result.whenComplete((matched, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertFalse(matched);
    });
  }
}
