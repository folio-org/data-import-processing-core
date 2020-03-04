package org.folio.processing.matching.loader;

import org.folio.MatchDetail;
import org.folio.processing.matching.loader.query.DefaultLoadQuery;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.loader.query.LoadQueryBuilder;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.Qualifier;
import org.folio.rest.jaxrs.model.StaticValueDetails;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Collections;

import static java.lang.String.format;
import static org.folio.MatchDetail.MatchCriterion.EXACTLY_MATCHES;
import static org.folio.MatchDetail.MatchCriterion.EXISTING_VALUE_BEGINS_WITH_INCOMING_VALUE;
import static org.folio.MatchDetail.MatchCriterion.EXISTING_VALUE_CONTAINS_INCOMING_VALUE;
import static org.folio.MatchDetail.MatchCriterion.EXISTING_VALUE_ENDS_WITH_INCOMING_VALUE;
import static org.folio.MatchDetail.MatchCriterion.INCOMING_VALUE_CONTAINS_EXISTING_VALUE;
import static org.folio.MatchDetail.MatchCriterion.INCOMING_VALUE_ENDS_WITH_EXISTING_VALUE;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.STATIC_VALUE;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class LoadQueryBuilderTest {

  @Test
  public void shouldBuildQueryWhere_ExistingValueExactlyMatches_IncomingStringValue() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("WHERE purchase_order.jsonb ->> 'poNumber' = '%s'", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCQLQuery = format("poNumber = '%s'", value.getValue());
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQuery_ExistingValueContains_IncomingStringValue() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXISTING_VALUE_CONTAINS_INCOMING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("WHERE purchase_order.jsonb ->> 'poNumber' LIKE '%%%s%%'", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCQLQuery = format("poNumber = '*%s*'", value.getValue());
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQuery_IncomingStringValue_ContainsExistingValue() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(INCOMING_VALUE_CONTAINS_EXISTING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("WHERE '%s' LIKE CONCAT('%%', purchase_order.jsonb ->> 'poNumber', '%%')", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCQLQuery = format("poNumber any '%s'", value.getValue());
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQuery_ExistingValueEndsWith_IncomingStringValue() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXISTING_VALUE_ENDS_WITH_INCOMING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("WHERE purchase_order.jsonb ->> 'poNumber' LIKE '%%%s'", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCQLQuery = format("poNumber = '*%s'", value.getValue());
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQuery_IncomingStringValueEndsWith_ExistingValue() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(INCOMING_VALUE_ENDS_WITH_EXISTING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("WHERE '%s' LIKE CONCAT('%%', purchase_order.jsonb ->> 'poNumber')", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCQLQuery = "";
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQuery_ExistingValueBeginsWith_IncomingStringValue() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXISTING_VALUE_BEGINS_WITH_INCOMING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("WHERE purchase_order.jsonb ->> 'poNumber' LIKE '%s%%'", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCQLQuery = format("poNumber = '%s*'", value.getValue());
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQuery_ExistingValueBeginsWith_IncomingListValue() {
    // given
    ListValue value = ListValue.of(Arrays.asList("ybp7406411", "NhCcYBP"));
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXISTING_VALUE_BEGINS_WITH_INCOMING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("WHERE (purchase_order.jsonb ->> 'poNumber' LIKE '%s%%' OR purchase_order.jsonb ->> 'poNumber' LIKE '%s%%')",
      value.getValue().get(0), value.getValue().get(1));
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCQLQuery = "";
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQuery_ExistingValueContains_IncomingStringValue_JsonArrayPath() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXISTING_VALUE_CONTAINS_INCOMING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("instance.subjects[]"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("CROSS JOIN LATERAL jsonb_array_elements_text(instance.jsonb -> 'subjects') fields(field) WHERE field LIKE '%%%s%%'", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
  }

  @Test
  public void shouldBuildQuery_IncomingStringValueEndsWith_ExistingValue_JsonArrayPath() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(INCOMING_VALUE_ENDS_WITH_EXISTING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("CROSS JOIN LATERAL jsonb_array_elements(instance.jsonb -> 'identifiers') fields(field) " +
      "WHERE '%s' LIKE CONCAT('%%', field ->> 'value')", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
  }

  @Test
  public void shouldBuildQuery_ExistingValueBeginsWith_IncomingListValue_JsonArrayPath() {
    // given
    ListValue value = ListValue.of(Arrays.asList("ybp7406411", "NhCcYBP"));
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXISTING_VALUE_BEGINS_WITH_INCOMING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("CROSS JOIN LATERAL jsonb_array_elements(instance.jsonb -> 'identifiers') fields(field) " +
        "WHERE (field ->> 'value' LIKE '%s%%' OR field ->> 'value' LIKE '%s%%')",
      value.getValue().get(0), value.getValue().get(1));
    assertEquals(expectedSQLQuery, result.getSql());
  }

  @Test
  public void shouldBuildQuery_ExistingValueBeginsWith_IncomingListValue_JsonArrayPath_WithQualifier() {
    // given
    ListValue value = ListValue.of(Arrays.asList("ybp7406411", "NhCcYBP"));
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXISTING_VALUE_BEGINS_WITH_INCOMING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withQualifier(new Qualifier()
          .withQualifierType(Qualifier.QualifierType.BEGINS_WITH)
          .withQualifierValue("978"))
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("CROSS JOIN LATERAL jsonb_array_elements(instance.jsonb -> 'identifiers') fields(field) " +
        "WHERE (field ->> 'value' LIKE '%s%%' OR field ->> 'value' LIKE '%s%%') AND LIKE '978%%'",
      value.getValue().get(0), value.getValue().get(1));
    assertEquals(expectedSQLQuery, result.getSql());
  }

  @Test
  public void shouldBuildQuery_ExistingValueBeginsWith_IncomingStringValue_WithQualifier() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXISTING_VALUE_BEGINS_WITH_INCOMING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withQualifier(new Qualifier()
          .withQualifierType(Qualifier.QualifierType.ENDS_WITH)
          .withQualifierValue("978"))
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("WHERE purchase_order.jsonb ->> 'poNumber' LIKE '%s%%' AND LIKE '%%978'", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCQLQuery = format("poNumber = '%s*' AND poNumber = '*978'", value.getValue());
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQuery_IncomingStringValueEndsWith_ExistingValue_WithQualifier() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(INCOMING_VALUE_ENDS_WITH_EXISTING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withQualifier(new Qualifier()
          .withQualifierType(Qualifier.QualifierType.CONTAINS)
          .withQualifierValue("978"))
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("WHERE '%s' LIKE CONCAT('%%', purchase_order.jsonb ->> 'poNumber') AND LIKE '%%978%%'", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
  }

  @Test
  public void shouldBuildQuery_IncomingStringValueEndsWith_ExistingValue_WithQualifierComparisonPart() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(INCOMING_VALUE_ENDS_WITH_EXISTING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withQualifier(new Qualifier()
          .withQualifierType(Qualifier.QualifierType.CONTAINS)
          .withQualifierValue("978")
          .withComparisonPart(Qualifier.ComparisonPart.NUMERICS_ONLY))
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("WHERE '%s' LIKE CONCAT('%%', REGEXP_REPLACE(purchase_order.jsonb ->> 'poNumber', '[^[:digit:]]','','g')) AND LIKE '%%978%%'", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
  }

  @Test
  public void shouldBuildQuery_ExistingValueBeginsWith_IncomingListValue_WithQualifierComparisonPart() {
    // given
    ListValue value = ListValue.of(Arrays.asList("ybp7406411", "NhCcYBP"));
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXISTING_VALUE_BEGINS_WITH_INCOMING_VALUE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withQualifier(new Qualifier()
          .withComparisonPart(Qualifier.ComparisonPart.ALPHANUMERICS_ONLY))
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("WHERE (REGEXP_REPLACE(purchase_order.jsonb ->> 'poNumber', '[^[:alnum:]]','','g') LIKE '%s%%' "
        + "OR REGEXP_REPLACE(purchase_order.jsonb ->> 'poNumber', '[^[:alnum:]]','','g') LIKE '%s%%')",
      value.getValue().get(0), value.getValue().get(1));
    assertEquals(expectedSQLQuery, result.getSql());
  }

  @Test
  public void shouldReturnNullIfPassedNullValue() {
    // given
    Value value = null;
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNull(result);
  }

  @Test
  public void shouldReturnNullIfPassedMissingValue() {
    // given
    Value value = MissingValue.getInstance();
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.poNumber"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNull(result);
  }

  @Test
  public void shouldReturnNullIfMatchingByStaticValue() {
    // given
    Value value = MissingValue.getInstance();
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(STATIC_VALUE)
        .withStaticValueDetails(
          new StaticValueDetails()));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNull(result);
  }

  @Test
  public void dummyTestForDefaultLoadQuery() {
    // given
    String tableName = "instance";
    String whereClause = "WHERE TABLE_NAME.hrid = 'ybp7406411'";
    //when
    LoadQuery result = new DefaultLoadQuery(tableName, whereClause);
    //then
    assertNotNull(result.getSql());
    assertEquals(whereClause.replace("TABLE_NAME", "instance"), result.getSql());
    try {
      result.getCql();
      fail();
    } catch (Exception e) {
      assertEquals(UnsupportedOperationException.class, e.getClass());
    }
  }

}
