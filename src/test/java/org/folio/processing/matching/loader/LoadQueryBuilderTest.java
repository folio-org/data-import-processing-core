package org.folio.processing.matching.loader;

import org.apache.commons.lang3.StringUtils;
import org.folio.MatchDetail;
import org.folio.processing.matching.loader.query.DefaultLoadQuery;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.loader.query.LoadQueryBuilder;
import org.folio.processing.value.DateValue;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.Qualifier;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.StaticValueDetails;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import static java.lang.String.format;
import static org.folio.MatchDetail.MatchCriterion.EXACTLY_MATCHES;
import static org.folio.MatchDetail.MatchCriterion.EXISTING_VALUE_BEGINS_WITH_INCOMING_VALUE;
import static org.folio.MatchDetail.MatchCriterion.EXISTING_VALUE_CONTAINS_INCOMING_VALUE;
import static org.folio.MatchDetail.MatchCriterion.EXISTING_VALUE_ENDS_WITH_INCOMING_VALUE;
import static org.folio.MatchDetail.MatchCriterion.INCOMING_VALUE_CONTAINS_EXISTING_VALUE;
import static org.folio.MatchDetail.MatchCriterion.INCOMING_VALUE_ENDS_WITH_EXISTING_VALUE;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.STATIC_VALUE;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class LoadQueryBuilderTest {

  private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd";

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
    String expectedCQLQuery = format("poNumber == \"%s\"", value.getValue());
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_ExistingValueExactlyMatches_MultipleIncomingStringValue() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue("439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = "CROSS JOIN LATERAL jsonb_array_elements(instance.jsonb -> 'identifiers') fields(field) WHERE field ->> 'value' = 'ybp7406411' AND  field ->> 'identifierTypeId' = '439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef'";
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCQLQuery = "identifiers=\"\\\"identifierTypeId\\\":\\\"439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef\\\"\" AND (identifiers=\"\\\"value\\\":\\\"ybp7406411\\\"\")";
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_ExistingValueExactlyMatches_MultipleIncomingListValue() {
    // given
    ListValue value = ListValue.of(Arrays.asList("ybp7406411", "ybp74064123"));
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue("439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = "CROSS JOIN LATERAL jsonb_array_elements(instance.jsonb -> 'identifiers') fields(field) WHERE (field ->> 'value' = 'ybp7406411' OR field ->> 'value' = 'ybp74064123') AND  field ->> 'identifierTypeId' = '439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef'";
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCQLQuery = "identifiers=\"\\\"identifierTypeId\\\":\\\"439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef\\\"\" AND (identifiers=\"\\\"value\\\":\\\"ybp7406411\\\"\" OR identifiers=\"\\\"value\\\":\\\"ybp74064123\\\"\")";
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
    String expectedCQLQuery = format("poNumber == \"*%s*\"", value.getValue());
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
    String expectedCQLQuery = format("poNumber any \"%s\"", value.getValue());
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
    String expectedCQLQuery = format("poNumber == \"*%s\"", value.getValue());
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
    String expectedCQLQuery = format("poNumber == \"%s*\"", value.getValue());
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
    String expectedCQLQuery = "(poNumber == \"ybp7406411*\" OR poNumber == \"NhCcYBP*\")";
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
  public void shouldBuildQuery_ExactlyMatchesWith_IncomingListValue_JsonArrayPath() {
    // given
    ListValue value = ListValue.of(Arrays.asList("ybp7406411", "NhCcYBP"));
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
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
    assertNotNull(result.getCql());
    String expectedSQLQuery = format("CROSS JOIN LATERAL jsonb_array_elements(instance.jsonb -> 'identifiers') fields(field) " +
        "WHERE (field ->> 'value' = '%s' OR field ->> 'value' = '%s')",
      value.getValue().get(0), value.getValue().get(1));
    String expectedCQLQuery = format("identifiers=\"\\\"value\\\":\\\"%s\\\"\" OR identifiers=\"\\\"value\\\":\\\"%s\\\"\"",
      value.getValue().get(0), value.getValue().get(1));
    assertEquals(expectedSQLQuery, result.getSql());
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQuery_ExactlyMatchesWith_IncomingListValue() {
    // given
    ListValue value = ListValue.of(Arrays.asList("ybp7406411", "NhCcYBP"));
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("instance.value"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    assertNotNull(result.getCql());
    String expectedSQLQuery = format("WHERE (instance.jsonb ->> 'value' = '%s' OR instance.jsonb ->> 'value' = '%s')",
      value.getValue().get(0), value.getValue().get(1));
    String expectedCQLQuery = format("(value == \"%s\" OR value == \"%s\")",
      value.getValue().get(0), value.getValue().get(1));
    assertEquals(expectedSQLQuery, result.getSql());
    assertEquals(expectedCQLQuery, result.getCql());
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
    String expectedCQLQuery = format("poNumber == \"%s*\" AND poNumber = '*978'", value.getValue());
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
    Value<?> value = null;
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
    Value<?> value = MissingValue.getInstance();
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
  public void shouldReturnNullIfMatchingByExistingStaticValue() {
    // given
    Value<?> value = MissingValue.getInstance();
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

  @Test
  public void shouldBuildQueryWhere_ExistingValueExactlyMatches_IncomingDateValue() throws ParseException {
    // given
    SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT_PATTERN);
    Date fromDate = df.parse("2020-04-01");
    Date toDate = df.parse("2020-04-30");
    DateValue value = DateValue.of(fromDate, toDate);
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("purchase_order.createdDate"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSqlQuery = format("WHERE purchase_order.jsonb ->> 'createdDate' >= '%s' AND purchase_order.jsonb ->> 'createdDate' <= '%sT23:59:59.999'",
      value.getFromDate(), value.getToDate());
    assertEquals(expectedSqlQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCQLQuery = format("createdDate >= \"%s\" AND createdDate <= \"%sT23:59:59.999\"", value.getFromDate(), value.getToDate());
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_ExistingValueExactlyMatches_IncomingStringValueJsonArrayElement() {
    // given
    StringValue value = StringValue.of("boo");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("holdings.electronicAccess[]"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("CROSS JOIN LATERAL jsonb_array_elements_text(holdings.jsonb -> 'electronicAccess') fields(field) WHERE field = '%s'", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCqlQuery = format("electronicAccess=\\\"%s\\\"", value.getValue());
    assertEquals(expectedCqlQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_ExistingValueExactlyMatches_IncomingStringValueJsonArrayPath() {
    // given
    StringValue value = StringValue.of("https://www.emeraldinsight.com/loi/jepp");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("holdings.electronicAccess[].uri"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("CROSS JOIN LATERAL jsonb_array_elements(holdings.jsonb -> 'electronicAccess') fields(field) WHERE field ->> 'uri' = '%s'", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCqlQuery = format("electronicAccess=\"\\\"uri\\\":\\\"%s\\\"\"", value.getValue());
    assertEquals(expectedCqlQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_ExistingValueExactlyMatches_IncomingStringValueWithSpecialCharactersJsonArrayPath() {
    // given
    StringValue value = StringValue.of("http://proxy2.missouristate.edu/login?url=https://fod.infobase.com/PortalPlaylists.aspx?wID=97835%26xtid=207292");
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Collections.singletonList(
          new Field().withLabel("field").withValue("holdings.electronicAccess[].uri"))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertNotNull(result.getSql());
    String expectedSQLQuery = format("CROSS JOIN LATERAL jsonb_array_elements(holdings.jsonb -> 'electronicAccess') fields(field) WHERE field ->> 'uri' = '%s'", value.getValue());
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotNull(result.getCql());
    String expectedCqlQuery = "electronicAccess=\"\\\"uri\\\":\\\"http://proxy2.missouristate.edu/login\\?url=https://fod.infobase.com/PortalPlaylists.aspx\\?wID=97835%26xtid=207292\\\"\"";
    assertEquals(expectedCqlQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_ExistingValueExactlyMatches_MultipleIncomingListValueWithNewCQLQuery() {
    // given
    StringValue value = StringValue.of("ybp7406411");
    String identifierTypeFieldValue = "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef";
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue(identifierTypeFieldValue))
        ));

    MatchDetail matchDetailWithWrongSecondField = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeIdr").withValue(identifierTypeFieldValue))
        ));

    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    LoadQuery wrongResult = LoadQueryBuilder.build(value, matchDetailWithWrongSecondField);
    //then
    assertNotNull(result);
    assertNotNull(wrongResult);
    assertNotNull(result.getSql());
    assertNotNull(wrongResult.getSql());
    String expectedSQLQuery = StringUtils.EMPTY;
    assertEquals(expectedSQLQuery, result.getSql());
    assertNotEquals(expectedSQLQuery, wrongResult.getSql());
    assertNotNull(result.getCql());
    assertNotNull(wrongResult.getCql());
    String expectedCQLQuery = format("identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"%s\\\"\"",identifierTypeFieldValue, value.getValue());
    assertEquals(expectedCQLQuery, result.getCql());
    assertNotEquals(expectedCQLQuery, wrongResult.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_IdentifierMatching_WithParenthesesInValue() {
    // given
    StringValue value = StringValue.of("(OCoLC)1024095011");
    String identifierTypeFieldValue = "7e591197-f335-4afb-bc6d-a6d76ca3bace";
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue(identifierTypeFieldValue))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(StringUtils.EMPTY, result.getSql());
    String expectedCQLQuery = format("identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"\\(OCoLC\\)1024095011\\\"\"", identifierTypeFieldValue);
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_IdentifierMatching_WithQuotesInValue() {
    // given
    StringValue value = StringValue.of("test\"quote\"value");
    String identifierTypeFieldValue = "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef";
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue(identifierTypeFieldValue))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(StringUtils.EMPTY, result.getSql());
    String expectedCQLQuery = format("identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"test\\\"quote\\\"value\\\"\"", identifierTypeFieldValue);
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_IdentifierMatching_WithBackslashesInValue() {
    // given
    StringValue value = StringValue.of("path\\to\\resource");
    String identifierTypeFieldValue = "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef";
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue(identifierTypeFieldValue))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(StringUtils.EMPTY, result.getSql());
    String expectedCQLQuery = format("identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"path\\\\to\\\\resource\\\"\"", identifierTypeFieldValue);
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_IdentifierMatching_WithWildcardsInValue() {
    // given
    StringValue value = StringValue.of("test*value?");
    String identifierTypeFieldValue = "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef";
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue(identifierTypeFieldValue))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(StringUtils.EMPTY, result.getSql());
    String expectedCQLQuery = format("identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"test\\*value\\?\\\"\"", identifierTypeFieldValue);
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_IdentifierMatching_WithMultipleSpecialCharacters() {
    // given
    StringValue value = StringValue.of("(test*)\\query?");
    String identifierTypeFieldValue = "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef";
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue(identifierTypeFieldValue))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(StringUtils.EMPTY, result.getSql());
    String expectedCQLQuery = format("identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"\\(test\\*\\)\\\\query\\?\\\"\"", identifierTypeFieldValue);
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_IdentifierMatching_ListWithSpecialCharacters() {
    // given
    ListValue value = ListValue.of(Arrays.asList("(OCoLC)123", "test*value", "path\\file"));
    String identifierTypeFieldValue = "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef";
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue(identifierTypeFieldValue))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(StringUtils.EMPTY, result.getSql());
    String expectedCQLQuery = format("(identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"\\(OCoLC\\)123\\\"\") OR (identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"test\\*value\\\"\") OR (identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"path\\\\file\\\"\")", identifierTypeFieldValue, identifierTypeFieldValue, identifierTypeFieldValue);
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_IdentifierMatching_WithApostropheInValue() {
    // given
    StringValue value = StringValue.of("O'Reilly's Book");
    String identifierTypeFieldValue = "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef";
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue(identifierTypeFieldValue))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(StringUtils.EMPTY, result.getSql());
    // Apostrophes don't need escaping in CQL
    String expectedCQLQuery = format("identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"O'Reilly's Book\\\"\"", identifierTypeFieldValue);
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_IdentifierMatching_EmptyValue() {
    // given
    StringValue value = StringValue.of("");
    String identifierTypeFieldValue = "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef";
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue(identifierTypeFieldValue))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(StringUtils.EMPTY, result.getSql());
    String expectedCQLQuery = format("identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"\\\"\"", identifierTypeFieldValue);
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_IdentifierMatching_PreEscapedValue() {
    // given
    StringValue value = StringValue.of("already\\\\escaped");
    String identifierTypeFieldValue = "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef";
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue(identifierTypeFieldValue))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(StringUtils.EMPTY, result.getSql());
    // Should double-escape the already escaped backslashes
    String expectedCQLQuery = format("identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"already\\\\\\\\escaped\\\"\"", identifierTypeFieldValue);
    assertEquals(expectedCQLQuery, result.getCql());
  }

  @Test
  public void shouldBuildQueryWhere_IdentifierMatching_RealWorldExampleFromProblem() {
    // Test the exact values from problem.md
    ListValue value = ListValue.of(Arrays.asList(
      "(CStRLIN)NYCX1604275S",
      "(NIC)notisABP6388",
      "366832",
      "(OCoLC)1604275"
    ));
    String identifierTypeFieldValue = "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef";
    MatchDetail matchDetail = new MatchDetail()
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(Arrays.asList(
          new Field().withLabel("field").withValue("instance.identifiers[].value"),
          new Field().withLabel("identifierTypeId").withValue(identifierTypeFieldValue))
        ));
    //when
    LoadQuery result = LoadQueryBuilder.build(value, matchDetail);
    //then
    assertNotNull(result);
    assertEquals(StringUtils.EMPTY, result.getSql());
    String expectedCQLQuery = format("(identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"\\(CStRLIN\\)NYCX1604275S\\\"\") OR (identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"\\(NIC\\)notisABP6388\\\"\") OR (identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"366832\\\"\") OR (identifiers=\"\\\"identifierTypeId\\\":\\\"%s\\\"\" AND identifiers=\"\\\"value\\\":\\\"\\(OCoLC\\)1604275\\\"\")", identifierTypeFieldValue, identifierTypeFieldValue, identifierTypeFieldValue, identifierTypeFieldValue);
    assertEquals(expectedCQLQuery, result.getCql());
  }

}
