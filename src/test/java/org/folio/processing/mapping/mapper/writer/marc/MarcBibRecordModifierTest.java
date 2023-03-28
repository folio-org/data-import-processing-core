package org.folio.processing.mapping.mapper.writer.marc;

import static io.vertx.core.json.jackson.DatabindCodec.mapper;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MappingDetail.MarcMappingOption.UPDATE;

import com.google.common.collect.Lists;
import io.vertx.core.json.Json;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.folio.DataImportEventPayload;
import org.folio.InstanceLinkDtoCollection;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.MappingProfile;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MarcBibRecordModifierTest extends MarcRecordModifierTest {

  private static final Integer LINKING_RULE_ID = 1;
  public static final String SUB_FIELD_CODE_A = "a";
  private final MarcBibRecordModifier marcBibRecordModifier;

  public MarcBibRecordModifierTest() {
    marcBibRecordModifier = new MarcBibRecordModifier();
    marcRecordModifier = marcBibRecordModifier;
  }

  //no mapping details tests
  @Test
  public void shouldRemoveLinksOnFieldsRemoval() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}]}";
    var expectedParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 0);
  }

  @Test
  public void shouldRemoveLinksOnRepeatableLinkedFieldRemoval() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00070nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book\"},{\"0\":\"test1\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00099nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book\"},{\"0\":\"test1\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 0);
  }

  @Test
  public void shouldNotUpdateLinkedSubfields() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test0\"},{\"9\":\"aabf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00150nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 1);
  }

  @Test
  public void shouldNotUpdateLinkedSubfield9() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"aaaf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00150nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 1);
  }

  @Test
  public void shouldNotRemoveLinkedSubfield9() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00150nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 1);
  }

  @Test
  public void shouldRemoveLinksOnSubfield0Removal() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00113nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 0);
  }

  @Test
  public void shouldRemoveLinksOnSeveralNotMatchedSubfield0() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test1\"},{\"0\":\"test2\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00127nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test1\"},{\"0\":\"test2\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 0);
  }

  @Test
  public void shouldRemoveUnmatchedSubfield0OnSeveralSubfield0() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test0\"},{\"0\":\"test2\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00150nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},"
      + "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},"
      + "{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}"
      + ",{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 1);
  }

  @Test
  public void shouldRemoveUnmatchedSubfield0OnSeveralSubfield0WhenMatchedIsLastOne() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test2\"},{\"0\":\"test0\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00150nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},"
      + "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},"
      + "{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}"
      + ",{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 1);
  }

  @Test
  public void shouldRemoveLinksOnSubfield0Change() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test1\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00120nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 0);
  }

  @Test
  public void shouldRemoveLinksOnSubfield0RemovalWith9SubfieldIncoming() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00113nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 0);
  }

  @Test
  public void shouldRemoveLinksOnSubfield0ChangeWith9SubfieldIncoming() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test1\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00120nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 0);
  }

  //custom mapping details tests
  @Test
  public void shouldNotUpdateLinkedSubfieldWhenOnlySubfieldMapped() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00171nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"110\":{\"subfields\":[{\"b\":\"book1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"111\":{\"subfields\":[{\"b\":\"book\"},{\"0\":\"test1\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, constructMappingDetails("a"), 1);
  }

  @Test
  public void shouldRemoveLinksWhenOnlySubfield0MappedAndChanged() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test1\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"100\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00133nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"110\":{\"subfields\":[{\"b\":\"book1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"111\":{\"subfields\":[{\"b\":\"book\"},{\"0\":\"test1\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, constructMappingDetails("0"), 0);
  }

  @Test
  public void shouldRemoveUncontrolledSubfields() throws IOException {
    // given
    var existingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\":\"tes\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}]}";
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}]}";
    var expectedParsedContent = "{\"leader\":\"00121nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    testMarcUpdating(existingParsedContent, incomingParsedContent, expectedParsedContent, emptyList(),emptyList(),emptyList(), 1, "100");
  }

  @Test
  public void shouldAdd9SubfieldToNotControllableField() throws IOException {
    // given
    var existingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\": \"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"101\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"101\":{\"subfields\":[{\"b\":\"book\"},{\"9\":\"aabf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00180nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"101\":{\"subfields\":[{\"b\":\"book\"},{\"9\":\"aabf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(existingParsedContent, incomingParsedContent, expectedParsedContent, emptyList(), emptyList(),emptyList(),1,"100");
  }

  @Test
  public void shouldAddMultiple9SubfieldsToNotControllableField() throws IOException {
    // given
    var existingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\": \"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"101\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"101\":{\"subfields\":[{\"b\":\"book\"},{\"9\":\"aabf59b7-913b-42ac-b1c6-e50ae7b00e6a\"},{\"9\":\"test\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00186nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"101\":{\"subfields\":[{\"b\":\"book\"},{\"9\":\"aabf59b7-913b-42ac-b1c6-e50ae7b00e6a\"},{\"9\":\"test\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(existingParsedContent, incomingParsedContent, expectedParsedContent, emptyList(), emptyList(),emptyList(),1, "100");
  }

  @Test
  public void shouldNotAdd9SubfieldToControllableField() throws IOException {
    // given
    var existingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\": \"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"110\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"110\":{\"subfields\":[{\"b\":\"book\"},{\"9\":\"aabf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00142nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"110\":{\"subfields\":[{\"b\":\"book\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(existingParsedContent, incomingParsedContent, expectedParsedContent, emptyList(), emptyList(),emptyList(),1, "100");
  }

  @Test
  public void shouldAddNewUncontrolledSubfields() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\":\"new subfield\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}]}";
    var expectedParsedContent = "{\"leader\":\"00135nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\":\"new subfield\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 1);
  }

  @Test
  public void shouldUpdateRepeatableUncontrolledSubfields() throws IOException {
    // given
    var existingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\": \"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"e\":\"e-value\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}]}";
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"e\":\"new subfield\"},{\"e\":\"e-value\"},{\"u\":\"u-value\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}]}";
    var expectedParsedContent = "{\"leader\":\"00153nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},"
      + "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"e\":\"new subfield\"},{\"e\":\"e-value\"},{\"u\":\"u-value\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    testMarcUpdating(existingParsedContent, incomingParsedContent, expectedParsedContent,emptyList(), emptyList(),emptyList(),1, "100");
  }

  //field protection settings tests
  @Test
  public void shouldRetainLinkIfNotRepeatableAndProtectedFieldChanged() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\":\"new subfield\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}]}";
    var expectedParsedContent = "{\"leader\":\"00121nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, constructMarcFieldProtectionSettings("100", false), emptyList(),1);
  }

  @Test
  public void shouldRetainLinkIfNotRepeatableAndProtectedFieldRemoved() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}]}";
    var expectedParsedContent = "{\"leader\":\"00121nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, constructMarcFieldProtectionSettings("100", false), emptyList(),1);
  }

  @Test
  public void shouldRetainLinkIfRepeatableAndProtectedFieldUpdated() throws IOException {
    // given
    var existingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\": \"ybp7406411\"}," +
      "{\"110\":{\"subfields\":[{\"b\":\"book1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"700\":{\"subfields\":[{\"a\":\"artistic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"700\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test1\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"111\":{\"subfields\":[{\"b\":\"book\"},{\"0\":\"test1\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"700\":{\"subfields\":[{\"a\":\"artistic\"},{\"b\":\"new subfield\"},{\"0\":\"test0\"}],\"ind1\": \" \",\"ind2\":\" \"}}]}";
    var expectedParsedContent = "{\"leader\":\"00237nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},"
      + "{\"700\":{\"subfields\":[{\"a\":\"artistic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}},"
      + "{\"700\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test1\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}},"
      + "{\"700\":{\"subfields\":[{\"a\":\"artistic\"},{\"b\":\"new subfield\"},{\"0\":\"test0\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    testMarcUpdating(existingParsedContent, incomingParsedContent, expectedParsedContent, emptyList(), constructMarcFieldProtectionSettings("700", false),
      emptyList(), 1, "700");
  }

  @Test
  public void shouldRetainLinkIfRepeatableAndProtectedFieldRemoved() throws IOException {
    // given
    var existingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\": \"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"110\":{\"subfields\":[{\"b\":\"book1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"700\":{\"subfields\":[{\"a\":\"artistic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"111\":{\"subfields\":[{\"b\":\"book\"},{\"0\":\"test1\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}]}";
    var expectedParsedContent = "{\"leader\":\"00119nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},"
      + "{\"700\":{\"subfields\":[{\"a\":\"artistic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    testMarcUpdating(existingParsedContent, incomingParsedContent, expectedParsedContent, emptyList(), constructMarcFieldProtectionSettings("700", false),
      emptyList(), 1, "700");;
  }

  @Test
  public void shouldRemoveLinkIfFieldIsProtectedByProtectionOverridden() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\":\"new subfield\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}]}";
    var expectedParsedContent = "{\"leader\":\"00135nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\":\"new subfield\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, constructMarcFieldProtectionSettings("100", false), constructMarcFieldProtectionSettings("100", true),1);
  }

  //negative tests
  @Test
  public void shouldThrowExceptionWhenInvalidEntityType() throws IOException {
    var entityTypes = Lists.newArrayList(EntityType.values());
    entityTypes.remove(MARC_BIBLIOGRAPHIC);
    for (var entityType : entityTypes) {
      testInvalidEntityType(entityType);
    }
  }

  private void testInvalidEntityType(EntityType entityType) throws IOException {
    var incomingRecord = new Record().withParsedRecord(new ParsedRecord().withContent(""));
    var existingRecord = new Record().withParsedRecord(new ParsedRecord().withContent(""));

    var eventPayload = new DataImportEventPayload();
    var context = new HashMap<String, String>();
    context.put(entityType.value(), Json.encodePrettily(incomingRecord));
    context.put("MATCHED_" + entityType.value(), Json.encodePrettily(existingRecord));
    eventPayload.setContext(context);
    var mappingProfile = new MappingProfile().withMappingDetails(new MappingDetail().withMarcMappingOption(UPDATE));

    var exceptionThrown = false;
    try {
      marcBibRecordModifier.initialize(eventPayload, new MappingParameters(), mappingProfile, entityType,
        new InstanceLinkDtoCollection(), new ArrayList<>());
    } catch (IllegalArgumentException ex) {
      Assert.assertTrue(ex.getMessage().endsWith("support only " + MARC_BIBLIOGRAPHIC.value()));
      exceptionThrown = true;
    }

    Assert.assertTrue("Exception not thrown for " + entityType.value(), exceptionThrown);
  }

  private void testMarcUpdating(String incomingParsedContent,
                                String expectedParsedContent,
                                int expectedLinksCount) throws IOException {
    testMarcUpdating(incomingParsedContent, expectedParsedContent, emptyList(), emptyList(), emptyList(),
      expectedLinksCount);
  }

  private void testMarcUpdating(String incomingParsedContent,
                                String expectedParsedContent,
                                List<MarcFieldProtectionSetting> systemProtectionSettings,
                                List<MarcFieldProtectionSetting> profileProtectionSettings,
                                int expectedLinksCount) throws IOException {
    testMarcUpdating(incomingParsedContent, expectedParsedContent, emptyList(), systemProtectionSettings,
      profileProtectionSettings, expectedLinksCount);
  }

  private void testMarcUpdating(String incomingParsedContent,
                                String expectedParsedContent,
                                List<MarcMappingDetail> mappingDetails,
                                int expectedLinksCount) throws IOException {
    testMarcUpdating(incomingParsedContent, expectedParsedContent, mappingDetails, emptyList(), emptyList(),
      expectedLinksCount);
  }

  private void testMarcUpdating(String incomingParsedContent,
                                String expectedParsedContent,
                                List<MarcMappingDetail> mappingDetails,
                                List<MarcFieldProtectionSetting> systemProtectionSettings,
                                List<MarcFieldProtectionSetting> profileProtectionSettings,
                                int expectedLinksCount) throws IOException {
    var existingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\": \"ybp7406411\"}," +
      "{\"100\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test0\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}},"
      +
      "{\"110\":{\"subfields\":[{\"b\":\"book1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"111\":{\"subfields\":[{\"b\":\"book\"},{\"0\":\"test1\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(existingParsedContent, incomingParsedContent, expectedParsedContent, mappingDetails,
      systemProtectionSettings, profileProtectionSettings,
      expectedLinksCount, "100");
  }

  private void testMarcUpdating(String existingParsedContent,
                                String incomingParsedContent,
                                String expectedParsedContent,
                                List<MarcMappingDetail> mappingDetails,
                                List<MarcFieldProtectionSetting> systemProtectionSettings,
                                List<MarcFieldProtectionSetting> profileProtectionSettings,
                                int expectedLinksCount, String... linkedTags) throws IOException {
    var incomingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(incomingParsedContent));
    var existingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(existingParsedContent));

    var eventPayload = new DataImportEventPayload();
    var context = new HashMap<String, String>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(incomingRecord));
    context.put(MATCHED_MARC_BIB_KEY, Json.encodePrettily(existingRecord));
    eventPayload.setContext(context);

    var mappingProfile = new MappingProfile()
      .withMarcFieldProtectionSettings(profileProtectionSettings)
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(UPDATE)
        .withMarcMappingDetails(mappingDetails));
    var mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(systemProtectionSettings);
    var links = constructLinkCollection(linkedTags);
    var linkingRules = constructLinkingRuleCollection(linkedTags);

    //when
    marcBibRecordModifier.initialize(eventPayload, mappingParameters, mappingProfile, MARC_BIBLIOGRAPHIC, links, linkingRules);
    marcBibRecordModifier.processUpdateMappingOption(mappingProfile.getMappingDetails().getMarcMappingDetails());
    marcBibRecordModifier.getResult(eventPayload);
    //then
    var recordJson = eventPayload.getContext().get(MATCHED_MARC_BIB_KEY);
    var actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
    Assert.assertEquals(expectedLinksCount, marcBibRecordModifier.getBibAuthorityLinksKept().size());
  }

  private List<LinkingRuleDto> constructLinkingRuleCollection(String[] bibRecordTag) {
    List<LinkingRuleDto> linkingRuleDtos = new ArrayList<>();
    for (int i = 0; i < bibRecordTag.length; i++) {
      LinkingRuleDto dto = new LinkingRuleDto();
      dto.setId(i);
      dto.setBibField(bibRecordTag[i]);
      dto.setAuthoritySubfields(List.of(SUB_FIELD_CODE_A));
      linkingRuleDtos.add(dto);
    }

    return linkingRuleDtos;
  }

  private List<MarcFieldProtectionSetting> constructMarcFieldProtectionSettings(String bibRecordTag, boolean override) {
    return List.of(new MarcFieldProtectionSetting().withField(bibRecordTag)
      .withSource(MarcFieldProtectionSetting.Source.USER)
      .withOverride(override)
      .withId("1")
      .withIndicator1("*")
      .withIndicator2("*")
      .withSubfield("9")
      .withData("*"));
  }

  private InstanceLinkDtoCollection constructLinkCollection(String... bibRecordTag) {
    List<Link> links = new ArrayList<>();
    for (int i = 0; i < bibRecordTag.length; i++) {
      Link link = constructLink(i);
      links.add(link);
    }
    return new InstanceLinkDtoCollection()
      .withLinks(links);
  }

  private Link constructLink(int id) {
    return new Link().withId(id)
      .withLinkingRuleId(id)
      .withAuthorityId(UUID.randomUUID().toString())
      .withInstanceId(UUID.randomUUID().toString())
      .withAuthorityNaturalId("test" + id);
  }

  private List<MarcMappingDetail> constructMappingDetails(String subfield) {
    return singletonList(new MarcMappingDetail()
      .withOrder(0)
      .withField(new MarcField()
        .withField("100")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(singletonList(
          new MarcSubfield().withSubfield(subfield)))));
  }
}
