package org.folio.processing.matching.matcher;

import io.vertx.core.json.JsonArray;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;
import static org.mockito.ArgumentMatchers.any;

@RunWith(VertxUnitRunner.class)
public class HoldingsItemMatcherTest {
  private static final String parsedContentWithMultiple = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"945\":{\"subfields\":[{\"a\":\"E\"},{\"s\":\"testCode\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"945\":{\"subfields\":[{\"a\":\"KU/CC/DI/A\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"945\":{\"subfields\":[{\"h\":\"KU/CC/DI/A\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
  private HoldingsItemMatcher matcher;
  private MatchValueLoader holdingsValueLoader;
  private MatchValueReader valueReader;

  @Before
  public void beforeTest() {
    holdingsValueLoader = Mockito.mock(MatchValueLoader.class);
    valueReader = Mockito.mock(MatchValueReader.class);

    Mockito.doAnswer(invocationOnMock -> {
      LoadResult loadResult = new LoadResult();
      loadResult.setValue("{\"permanentLocationId\": \"testId\"}");
      loadResult.setEntityType(HOLDINGS.value());
      return CompletableFuture.completedFuture(loadResult);
    }).when(holdingsValueLoader).loadEntity(any(), any());

    Mockito.doAnswer(invocationOnMock -> ListValue.of(List.of("test1", "test2", "test3"))).when(valueReader).read(any(), any());
    Mockito.doAnswer(invocationOnMock -> true).when(valueReader).isEligibleForEntityType(any());
    matcher = new HoldingsItemMatcher(valueReader, holdingsValueLoader);
  }

  @Test
  public void shouldNotMatchSingleHoldings(TestContext testContext) {
    Async async = testContext.async();
    Mockito.doAnswer(invocationOnMock -> StringValue.of("test1")).when(valueReader).read(any(), any());

    Mockito.doAnswer(invocationOnMock -> {
      LoadResult loadResult = new LoadResult();
      loadResult.setValue(null);
      loadResult.setEntityType(HOLDINGS.value());
      return CompletableFuture.completedFuture(loadResult);
    }).when(holdingsValueLoader).loadEntity(any(), any());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(HOLDINGS)
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchDetails(Collections.singletonList(new MatchDetail().withExistingRecordType(HOLDINGS)
        .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
        .withExistingMatchExpression(new MatchExpression().withFields(List.of(new Field().withValue("945"))))));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), parsedContentWithMultiple);
    context.put("NOT_MATCHED_NUMBER", "3");
    context.put("MAPPING_PARAMS", "{}");
    context.put("MATCHING_PARAMETERS_RELATIONS", "{}");

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(matchProfileWrapper);

    CompletableFuture<Boolean> result = matcher.match(eventPayload);

    result.whenComplete((matched, throwable) -> {
      JsonArray holdings = new JsonArray(eventPayload.getContext().get(HOLDINGS.value()));
      testContext.assertEquals(0, holdings.size());
      testContext.assertEquals("1", eventPayload.getContext().get("NOT_MATCHED_NUMBER"));
      testContext.assertNull(throwable);
      testContext.assertFalse(matched);
      async.complete();
    });
  }

  @Test
  public void shouldMatchSingleHoldings(TestContext testContext) {
    Async async = testContext.async();
    Mockito.doAnswer(invocationOnMock -> StringValue.of("test1")).when(valueReader).read(any(), any());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(HOLDINGS)
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchDetails(Collections.singletonList(new MatchDetail().withExistingRecordType(HOLDINGS)
        .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
        .withExistingMatchExpression(new MatchExpression().withFields(List.of(new Field().withValue("945"))))));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), parsedContentWithMultiple);
    context.put("NOT_MATCHED_NUMBER", "3");
    context.put("MAPPING_PARAMS", "{}");
    context.put("MATCHING_PARAMETERS_RELATIONS", "{}");

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(matchProfileWrapper);

    CompletableFuture<Boolean> result = matcher.match(eventPayload);

    result.whenComplete((matched, throwable) -> {
      JsonArray holdings = new JsonArray(eventPayload.getContext().get(HOLDINGS.value()));
      testContext.assertEquals(1, holdings.size());
      testContext.assertNull(eventPayload.getContext().get("NOT_MATCHED_NUMBER"));
      testContext.assertNull(throwable);
      testContext.assertTrue(matched);
      async.complete();
    });
  }

  @Test
  public void shouldMatchSingleItem(TestContext testContext) {
    Async async = testContext.async();
    Mockito.doAnswer(invocationOnMock -> StringValue.of("test1")).when(valueReader).read(any(), any());

    Mockito.doAnswer(invocationOnMock -> {
      LoadResult loadResult = new LoadResult();
      loadResult.setValue("{\"permanentLocationId\": \"testId\"}");
      loadResult.setEntityType(ITEM.value());
      return CompletableFuture.completedFuture(loadResult);
    }).when(holdingsValueLoader).loadEntity(any(), any());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(ITEM)
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchDetails(Collections.singletonList(new MatchDetail().withExistingRecordType(ITEM)
        .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
        .withExistingMatchExpression(new MatchExpression().withFields(List.of(new Field().withValue("945"))))));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), parsedContentWithMultiple);
    context.put("NOT_MATCHED_NUMBER", "3");
    context.put("MAPPING_PARAMS", "{}");
    context.put("MATCHING_PARAMETERS_RELATIONS", "{}");

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(matchProfileWrapper);

    CompletableFuture<Boolean> result = matcher.match(eventPayload);

    result.whenComplete((matched, throwable) -> {
      JsonArray items = new JsonArray(eventPayload.getContext().get(ITEM.value()));
      testContext.assertEquals(1, items.size());
      testContext.assertNull(throwable);
      testContext.assertTrue(matched);
      async.complete();
    });
  }

  @Test
  public void shouldMatchMultipleHoldings(TestContext testContext) {
    Async async = testContext.async();
    Mockito.doAnswer(invocationOnMock -> ListValue.of(List.of("test1", "test2", "test3", "test3"))).when(valueReader).read(any(), any());
    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(HOLDINGS)
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchDetails(Collections.singletonList(new MatchDetail().withExistingRecordType(HOLDINGS).withIncomingRecordType(MARC_BIBLIOGRAPHIC)));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), parsedContentWithMultiple);
    context.put("NOT_MATCHED_NUMBER", "3");

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(matchProfileWrapper);

    CompletableFuture<Boolean> result = matcher.match(eventPayload);

    result.whenComplete((matched, throwable) -> {
      JsonArray holdings = new JsonArray(eventPayload.getContext().get(HOLDINGS.value()));
      testContext.assertEquals(3, holdings.size());
      testContext.assertNull(throwable);
      testContext.assertTrue(matched);
      testContext.assertEquals("0", eventPayload.getContext().get("NOT_MATCHED_NUMBER"));
      async.complete();
    });
  }

  @Test
  public void shouldMatchMultipleItems(TestContext testContext) {
    Async async = testContext.async();
    Mockito.doAnswer(invocationOnMock -> {
      LoadResult loadResult = new LoadResult();
      loadResult.setValue("{\"permanentLocationId\": \"testId\"}");
      loadResult.setEntityType(ITEM.value());
      return CompletableFuture.completedFuture(loadResult);
    }).when(holdingsValueLoader).loadEntity(any(), any());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(ITEM)
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchDetails(Collections.singletonList(new MatchDetail().withExistingRecordType(ITEM).withIncomingRecordType(MARC_BIBLIOGRAPHIC)));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), parsedContentWithMultiple);
    context.put("NOT_MATCHED_NUMBER", "3");

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(matchProfileWrapper);

    CompletableFuture<Boolean> result = matcher.match(eventPayload);

    result.whenComplete((matched, throwable) -> {
      JsonArray items = new JsonArray(eventPayload.getContext().get(ITEM.value()));
      testContext.assertEquals(3, items.size());
      testContext.assertNull(throwable);
      testContext.assertTrue(matched);
      testContext.assertEquals("0", eventPayload.getContext().get("NOT_MATCHED_NUMBER"));
      async.complete();
    });
  }

  @Test
  public void shouldFailMatchWhenErrorsForEachHolding(TestContext testContext) {
    Async async = testContext.async();
    Mockito.doAnswer(invocationOnMock -> {
      CompletableFuture<LoadResult> future = new CompletableFuture<>();
      future.completeExceptionally(new MatchingException("Error"));
      return future;
    }).when(holdingsValueLoader).loadEntity(any(), any());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(HOLDINGS)
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchDetails(Collections.singletonList(new MatchDetail().withExistingRecordType(HOLDINGS).withIncomingRecordType(MARC_BIBLIOGRAPHIC)));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), parsedContentWithMultiple);

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(matchProfileWrapper);

    CompletableFuture<Boolean> result = matcher.match(eventPayload);

    result.whenComplete((matched, throwable) -> {
      testContext.assertNotNull(throwable);
      testContext.assertNull(matched);
      JsonArray errors = new JsonArray(throwable.getMessage());
      testContext.assertEquals(3, errors.size());
      async.complete();
    });
  }

  @Test
  public void shouldNotMatchWhenNoHoldingsFound(TestContext testContext) {
    Async async = testContext.async();
    Mockito.doAnswer(invocationOnMock -> {
      LoadResult loadResult = new LoadResult();
      loadResult.setValue(null);
      loadResult.setEntityType(HOLDINGS.value());
      return CompletableFuture.completedFuture(loadResult);
    }).when(holdingsValueLoader).loadEntity(any(), any());

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(HOLDINGS)
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchDetails(Collections.singletonList(new MatchDetail().withExistingRecordType(HOLDINGS).withIncomingRecordType(MARC_BIBLIOGRAPHIC)));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), parsedContentWithMultiple);

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(matchProfileWrapper);

    CompletableFuture<Boolean> result = matcher.match(eventPayload);

    result.whenComplete((matched, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertFalse(matched);
      testContext.assertEquals("3", eventPayload.getContext().get("NOT_MATCHED_NUMBER"));
      async.complete();
    });
  }

  @Test
  public void shouldMatchAndReturnPartialErrorsForFailedHoldings(TestContext testContext) {
    Async async = testContext.async();
    CompletableFuture<LoadResult> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new MatchingException("Error"));

    LoadResult loadResult = new LoadResult();
    loadResult.setValue("{\"permanentLocationId\": \"testId\"}");
    loadResult.setEntityType(HOLDINGS.value());

    CompletableFuture<LoadResult> completedFuture = new CompletableFuture<>();
    completedFuture.complete(loadResult);

    Mockito.when(holdingsValueLoader.loadEntity(any(), any()))
      .thenReturn(errorFuture)
      .thenReturn(completedFuture)
      .thenReturn(completedFuture);

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(HOLDINGS)
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchDetails(Collections.singletonList(new MatchDetail().withExistingRecordType(HOLDINGS).withIncomingRecordType(MARC_BIBLIOGRAPHIC)));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), parsedContentWithMultiple);

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(matchProfileWrapper);

    CompletableFuture<Boolean> result = matcher.match(eventPayload);

    result.whenComplete((matched, throwable) -> {
      JsonArray holdings = new JsonArray(eventPayload.getContext().get(HOLDINGS.value()));
      testContext.assertEquals(2, holdings.size());
      JsonArray errors = new JsonArray(eventPayload.getContext().get("ERRORS"));
      testContext.assertEquals(1, errors.size());
      testContext.assertNull(throwable);
      testContext.assertTrue(matched);
      testContext.assertEquals("0", eventPayload.getContext().get("NOT_MATCHED_NUMBER"));
      async.complete();
    });
  }

  @Test
  public void shouldNonMatchAndReturnPartialErrorsForFailedHoldings(TestContext testContext) {
    Async async = testContext.async();
    Mockito.doAnswer(invocationOnMock -> ListValue.of(List.of("test1", "test2", "test3", "test4"))).when(valueReader).read(any(), any());
    CompletableFuture<LoadResult> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new MatchingException("Error"));

    LoadResult loadResultNonMatched = new LoadResult();
    loadResultNonMatched.setEntityType(HOLDINGS.value());
    loadResultNonMatched.setValue(null);

    CompletableFuture<LoadResult> completedFutureNonMatched = new CompletableFuture<>();
    completedFutureNonMatched.complete(loadResultNonMatched);

    Mockito.when(holdingsValueLoader.loadEntity(any(), any()))
      .thenReturn(errorFuture)
      .thenReturn(errorFuture)
      .thenReturn(completedFutureNonMatched)
      .thenReturn(completedFutureNonMatched);

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(HOLDINGS)
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchDetails(Collections.singletonList(new MatchDetail().withExistingRecordType(HOLDINGS).withIncomingRecordType(MARC_BIBLIOGRAPHIC)));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), parsedContentWithMultiple);
    context.put(HOLDINGS.value(), "[]");

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(matchProfileWrapper);

    CompletableFuture<Boolean> result = matcher.match(eventPayload);

    result.whenComplete((matched, throwable) -> {
      JsonArray holdings = new JsonArray(eventPayload.getContext().get(HOLDINGS.value()));
      testContext.assertEquals(0, holdings.size());
      JsonArray errors = new JsonArray(eventPayload.getContext().get("ERRORS"));
      testContext.assertEquals(2, errors.size());
      testContext.assertNull(throwable);
      testContext.assertFalse(matched);
      testContext.assertEquals("2", eventPayload.getContext().get("NOT_MATCHED_NUMBER"));
      async.complete();
    });
  }


  @Test
  public void shouldMatchAndReturnPartialErrorsForFailedHoldingsAndSetNumberOfNonMatchedHoldingsInContext(TestContext testContext) {
    Async async = testContext.async();
    CompletableFuture<LoadResult> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new MatchingException("Error"));

    LoadResult loadResultMatched = new LoadResult();
    loadResultMatched.setEntityType(HOLDINGS.value());
    loadResultMatched.setValue("{\"permanentLocationId\": \"testId\"}");

    LoadResult loadResultNonMatched = new LoadResult();
    loadResultNonMatched.setEntityType(HOLDINGS.value());
    loadResultNonMatched.setValue(null);

    CompletableFuture<LoadResult> completedFutureMatched = new CompletableFuture<>();
    completedFutureMatched.complete(loadResultMatched);

    CompletableFuture<LoadResult> completedFutureNonMatched = new CompletableFuture<>();
    completedFutureNonMatched.complete(loadResultNonMatched);

    Mockito.when(holdingsValueLoader.loadEntity(any(), any()))
      .thenReturn(errorFuture)
      .thenReturn(completedFutureMatched)
      .thenReturn(completedFutureNonMatched);

    MatchProfile matchProfile = new MatchProfile()
      .withExistingRecordType(HOLDINGS)
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchDetails(Collections.singletonList(new MatchDetail().withExistingRecordType(HOLDINGS).withIncomingRecordType(MARC_BIBLIOGRAPHIC)));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper();
    matchProfileWrapper.setContent(matchProfile);
    matchProfileWrapper.setContentType(MATCH_PROFILE);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), parsedContentWithMultiple);

    DataImportEventPayload eventPayload = new DataImportEventPayload();
    eventPayload.setContext(context);
    eventPayload.setCurrentNode(matchProfileWrapper);

    CompletableFuture<Boolean> result = matcher.match(eventPayload);

    result.whenComplete((matched, throwable) -> {
      JsonArray holdings = new JsonArray(eventPayload.getContext().get(HOLDINGS.value()));
      testContext.assertEquals(1, holdings.size());
      JsonArray errors = new JsonArray(eventPayload.getContext().get("ERRORS"));
      testContext.assertEquals(1, errors.size());
      testContext.assertNull(throwable);
      testContext.assertTrue(matched);
      testContext.assertEquals("1", eventPayload.getContext().get("NOT_MATCHED_NUMBER"));
      async.complete();
    });
  }
}
