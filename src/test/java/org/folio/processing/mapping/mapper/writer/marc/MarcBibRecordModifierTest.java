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
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.folio.DataImportEventPayload;
import org.folio.InstanceLinkDtoCollection;
import org.folio.Link;
import org.folio.MappingProfile;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MarcBibRecordModifierTest extends MarcRecordModifierTest {

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
      "{\"020\":{\"subfields\":[{\"b\":\"book1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book\"},{\"0\":\"test1\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00099nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book\"},{\"0\":\"test1\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 0);
  }

  @Test
  public void shouldNotUpdateLinkedSubfields() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00149nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 1);
  }

  @Test
  public void shouldNotUpdateLinkedSubfield9() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test\"},{\"9\":\"aaaf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00149nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 1);
  }

  @Test
  public void shouldNotRemoveLinkedSubfield9() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00149nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 1);
  }

  @Test
  public void shouldRemoveLinksOnSubfield0Removal() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic updated\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00113nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic updated\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 0);
  }

  @Test
  public void shouldRemoveLinksOnSubfield0Change() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test1\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00120nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 0);
  }

  @Test
  public void shouldRemoveLinksOnSubfield0RemovalWith9SubfieldIncoming() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00113nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic updated\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 0);
  }

  @Test
  public void shouldRemoveLinksOnSubfield0ChangeWith9SubfieldIncoming() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test1\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00120nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 0);
  }

  //custom mapping details tests
  @Test
  public void shouldNotUpdateLinkedSubfieldWhenOnlySubfieldMapped() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00170nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book\"},{\"0\":\"test1\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, constructMappingDetails("a"), 1);
  }

  @Test
  public void shouldRemoveLinksWhenOnlySubfield0MappedAndChanged() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic updated\"},{\"0\":\"test1\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book updated\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var expectedParsedContent = "{\"leader\":\"00133nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book\"},{\"0\":\"test1\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, constructMappingDetails("0"), 0);
  }

  @Test
  public void shouldAddNewUncontrolledSubfields() throws IOException {
    // given
    var incomingParsedContent = "{\"leader\":\"00049nam  22000371a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\":\"new subfield\"},{\"0\":\"test\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}]}";
    var expectedParsedContent = "{\"leader\":\"00134nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"b\":\"new subfield\"},{\"0\":\"test\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    testMarcUpdating(incomingParsedContent, expectedParsedContent, 1);
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
      marcBibRecordModifier.initialize(eventPayload, new MappingParameters(), mappingProfile, entityType, new InstanceLinkDtoCollection());
    } catch (IllegalArgumentException ex) {
      Assert.assertTrue(ex.getMessage().endsWith("support only " + MARC_BIBLIOGRAPHIC.value()));
      exceptionThrown = true;
    }
    Assert.assertTrue("Exception not thrown for " + entityType.value(), exceptionThrown);
  }

  private void testMarcUpdating(String incomingParsedContent,
                                String expectedParsedContent,
                                int expectedLinksCount) throws IOException {
    testMarcUpdating(incomingParsedContent, expectedParsedContent, emptyList(), expectedLinksCount);
  }

  private void testMarcUpdating(String incomingParsedContent,
                                String expectedParsedContent,
                                List<MarcMappingDetail> mappingDetails,
                                int expectedLinksCount) throws IOException {
    var incomingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(incomingParsedContent));
    var existingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\": \"ybp7406411\"}," +
      "{\"020\":{\"subfields\":[{\"a\":\"electronic\"},{\"0\":\"test\"},{\"9\":\"bdbf59b7-913b-42ac-b1c6-e50ae7b00e6a\"}],\"ind1\": \" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book1\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"020\":{\"subfields\":[{\"b\":\"book\"},{\"0\":\"test1\"}],\"ind1\":\"0\",\"ind2\":\"0\"}}]}";
    var existingRecord = new Record().withParsedRecord(new ParsedRecord()
      .withContent(existingParsedContent));

    var eventPayload = new DataImportEventPayload();
    var context = new HashMap<String, String>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encodePrettily(incomingRecord));
    context.put(MATCHED_MARC_BIB_KEY, Json.encodePrettily(existingRecord));
    eventPayload.setContext(context);

    var mappingProfile = new MappingProfile()
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(UPDATE)
        .withMarcMappingDetails(mappingDetails));
    var mappingParameters = new MappingParameters();
    var links = constructLinkCollection("020");

    //when
    marcBibRecordModifier.initialize(eventPayload, mappingParameters, mappingProfile, MARC_BIBLIOGRAPHIC, links);
    marcBibRecordModifier.processUpdateMappingOption(mappingProfile.getMappingDetails().getMarcMappingDetails());
    marcBibRecordModifier.getResult(eventPayload);
    //then
    var recordJson = eventPayload.getContext().get(MATCHED_MARC_BIB_KEY);
    var actualRecord = mapper().readValue(recordJson, Record.class);
    Assert.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
    Assert.assertEquals(expectedLinksCount, marcBibRecordModifier.getBibAuthorityLinksKept().size());
  }

  private InstanceLinkDtoCollection constructLinkCollection(String bibRecordTag) {
    return new InstanceLinkDtoCollection()
      .withLinks(singletonList(constructLink(bibRecordTag)));
  }

  private Link constructLink(String bibRecordTag) {
    return new Link().withId(nextInt())
      .withBibRecordTag(bibRecordTag)
      .withBibRecordSubfields(singletonList("a"))
      .withAuthorityId(UUID.randomUUID().toString())
      .withInstanceId(UUID.randomUUID().toString())
      .withAuthorityNaturalId("test");
  }

  private List<MarcMappingDetail> constructMappingDetails(String subfield) {
    return singletonList(new MarcMappingDetail()
      .withOrder(0)
      .withField(new MarcField()
        .withField("020")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(singletonList(
          new MarcSubfield().withSubfield(subfield)))));
  }
}
