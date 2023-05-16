package org.folio.processing.matching.matcher;

import io.vertx.core.json.JsonArray;
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
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
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
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;
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

    Mockito.doAnswer(invocationOnMock -> {
      LoadResult loadResult = new LoadResult();
      loadResult.setValue("Some Value");
      loadResult.setEntityType(HOLDINGS.value());
      return CompletableFuture.completedFuture(loadResult);
    }).when(holdingsValueLoader).loadEntity(any(), any());

    valueReader = new MatchValueReader() {
      @Override
      public Value read(DataImportEventPayload eventPayload, MatchDetail matchDetail) {
        return ListValue.of(List.of("test1", "test2", "test3"));
      }

      @Override
      public boolean isEligibleForEntityType(EntityType incomingRecordType) {
        return incomingRecordType == MARC_BIBLIOGRAPHIC;
      }
    };

    matcher = new HoldingsItemMatcher(valueReader, holdingsValueLoader);
  }

  @Test
  public void shouldMatchMultipleHoldings(TestContext testContext) {
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
    });
  }

  @Test
  public void shouldFailMatchWhenErrorsForEachHolding(TestContext testContext) {
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
    });
  }

  @Test
  public void shouldNotMatchWhenNoHoldingsFound(TestContext testContext) {
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
    });
  }

  @Test
  public void shouldMatchAndReturnPartialErrorsForFailedHoldings(TestContext testContext) {
    CompletableFuture<LoadResult> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new MatchingException("Error"));

    LoadResult loadResult = new LoadResult();
    loadResult.setValue("Some Value");
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
    });
  }

  @Test
  public void shouldNonMatchAndReturnPartialErrorsForFailedHoldings(TestContext testContext) {
    CompletableFuture<LoadResult> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new MatchingException("Error"));

    LoadResult loadResultNonMatched = new LoadResult();
    loadResultNonMatched.setEntityType(HOLDINGS.value());
    loadResultNonMatched.setValue(null);

    CompletableFuture<LoadResult> completedFutureNonMatched = new CompletableFuture<>();
    completedFutureNonMatched.complete(loadResultNonMatched);

    Mockito.when(holdingsValueLoader.loadEntity(any(), any()))
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
      testContext.assertEquals(1, errors.size());
      testContext.assertNull(throwable);
      testContext.assertFalse(matched);
      testContext.assertEquals("2", eventPayload.getContext().get("NOT_MATCHED_NUMBER"));
    });
  }


  @Test
  public void shouldMatchAndReturnPartialErrorsForFailedHoldingsAndSetNumberOfNonMatchedHoldingsInContext(TestContext testContext) {
    CompletableFuture<LoadResult> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new MatchingException("Error"));

    LoadResult loadResultMatched = new LoadResult();
    loadResultMatched.setEntityType(HOLDINGS.value());
    loadResultMatched.setValue("Some Value");

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
    });
  }
}
