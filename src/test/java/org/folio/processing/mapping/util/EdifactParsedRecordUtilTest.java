package org.folio.processing.mapping.util;

import org.folio.ParsedRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;


public class EdifactParsedRecordUtilTest {

  private static final String EDIFACT_PARSED_CONTENT = "{\"segments\": [{\"tag\": \"UNA\", \"dataElements\": []}, {\"tag\": \"UNB\", \"dataElements\": [{\"components\": [{\"data\": \"UNOC\"}, {\"data\": \"3\"}]}, {\"components\": [{\"data\": \"EBSCO\"}, {\"data\": \"92\"}]}, {\"components\": [{\"data\": \"KOH0002\"}, {\"data\": \"91\"}]}, {\"components\": [{\"data\": \"200610\"}, {\"data\": \"0105\"}]}, {\"components\": [{\"data\": \"5162\"}]}]}, {\"tag\": \"UNH\", \"dataElements\": [{\"components\": [{\"data\": \"5162\"}]}, {\"components\": [{\"data\": \"INVOIC\"}, {\"data\": \"D\"}, {\"data\": \"96A\"}, {\"data\": \"UN\"}, {\"data\": \"EAN008\"}]}]}, {\"tag\": \"BGM\", \"dataElements\": [{\"components\": [{\"data\": \"380\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"JINV\"}]}, {\"components\": [{\"data\": \"0704159\"}]}, {\"components\": [{\"data\": \"43\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"137\"}, {\"data\": \"20191002\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"NAD\", \"dataElements\": [{\"components\": [{\"data\": \"BY\"}]}, {\"components\": [{\"data\": \"BR1624506\"}, {\"data\": \"\"}, {\"data\": \"91\"}]}]}, {\"tag\": \"NAD\", \"dataElements\": [{\"components\": [{\"data\": \"SR\"}]}, {\"components\": [{\"data\": \"EBSCO\"}, {\"data\": \"\"}, {\"data\": \"92\"}]}]}, {\"tag\": \"CUX\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"004362033\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1941-6067\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1941-6067(20200101)14;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1941-6067(20201231)14;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"ACADEMY OF MANAGEMENT ANNALS -   ON\"}, {\"data\": \"LINE FOR INSTITUTIONS\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200101\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20201231\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"208.59\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"205\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S255699\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"SNA\"}, {\"data\": \"C6546362\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"LINE SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"3.59\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"006288237\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1944-737X\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1944-737X(20200301)117;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1944-737X(20210228)118;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"ACI MATERIALS JOURNAL - ONLINE   -\"}, {\"data\": \"MULTI USER\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200301\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20210228\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"726.5\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"714\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S283902\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"LINE SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"12.5\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"3\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"006289532\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1944-7361\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1944-7361(20200301)117;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1944-7361(20210228)118;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"GRADUATE PROGRAMS IN PHYSICS, ASTRO\"}, {\"data\": \"NOMY AND \"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"RELATED FIELDS.\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200301\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20210228\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"726.5\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"714\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S283901\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"SNA\"}, {\"data\": \"E9498296\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"LINE SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"12.5\"}]}]}, {\"tag\": \"UNS\", \"dataElements\": [{\"components\": [{\"data\": \"S\"}]}]}, {\"tag\": \"CNT\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}, {\"data\": \"3\"}]}]}, {\"tag\": \"CNT\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}, {\"data\": \"3\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"79\"}, {\"data\": \"18929.07\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"9\"}, {\"data\": \"18929.07\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"TOTAL SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"325.59\"}]}]}, {\"tag\": \"UNT\", \"dataElements\": [{\"components\": [{\"data\": \"294\"}]}, {\"components\": [{\"data\": \"5162-1\"}]}]}, {\"tag\": \"UNZ\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}]}, {\"components\": [{\"data\": \"5162\"}]}]}]}";

  @Test
  public void shouldReturnCorrespondingValueForAllInvoiceLines() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);

    Map<Integer, String> actualSegmentsValues = EdifactParsedRecordUtil.getInvoiceLinesSegmentsValues(parsedRecord, "RFF+LI[2]");

    Assert.assertEquals(3, actualSegmentsValues.size());
    Assert.assertEquals("S255699", actualSegmentsValues.get(1));
    Assert.assertEquals("S283902", actualSegmentsValues.get(2));
    Assert.assertEquals("S283901", actualSegmentsValues.get(3));
  }

  @Test
  public void shouldReturnValuesForExistingInvoiceLinesSegments() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);

    Map<Integer, String> actualSegmentsValues = EdifactParsedRecordUtil.getInvoiceLinesSegmentsValues(parsedRecord, "RFF+SNA[2]");

    Assert.assertEquals(2, actualSegmentsValues.size());
    Assert.assertEquals("C6546362", actualSegmentsValues.get(1));
    Assert.assertEquals("E9498296", actualSegmentsValues.get(3));
  }

  @Test
  public void shouldReturnValuesWhenMappingExpressionHasQualifier() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);

    Map<Integer, String> actualSegmentsValues = EdifactParsedRecordUtil.getInvoiceLinesSegmentsValues(parsedRecord, "MOA+203?4[2]");

    Assert.assertEquals(3, actualSegmentsValues.size());
    Assert.assertEquals("208.59", actualSegmentsValues.get(1));
    Assert.assertEquals("726.5", actualSegmentsValues.get(2));
    Assert.assertEquals("726.5", actualSegmentsValues.get(3));
  }

  @Test
  public void shouldReturnValuesByDataPositionsRange() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);

    Map<Integer, String> actualSegmentsValues = EdifactParsedRecordUtil.getInvoiceLinesSegmentsValues(parsedRecord, "IMD+L+050+[4-5]");

    Assert.assertEquals(3, actualSegmentsValues.size());
    Assert.assertEquals("ACADEMY OF MANAGEMENT ANNALS -   ONLINE FOR INSTITUTIONS", actualSegmentsValues.get(1));
    Assert.assertEquals("ACI MATERIALS JOURNAL - ONLINE   -MULTI USER", actualSegmentsValues.get(2));
    Assert.assertEquals("GRADUATE PROGRAMS IN PHYSICS, ASTRONOMY AND RELATED FIELDS.", actualSegmentsValues.get(3));
  }

  @Test
  public void shouldReturnEmptyMapWhenInvoiceLinesHaveNoSpecifiedSegment() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);

    Map<Integer, String> actualSegmentsValues = EdifactParsedRecordUtil.getInvoiceLinesSegmentsValues(parsedRecord, "IMD+F+050+[4]");

    Assert.assertTrue(actualSegmentsValues.isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenParsedRecordHasNoParsedContent() {
    ParsedRecord parsedRecord = new ParsedRecord();
    EdifactParsedRecordUtil.getInvoiceLinesSegmentsValues(parsedRecord, "RFF+SNA[2]");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenMappingExpressionHasInvalidPositionsRange() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);
    EdifactParsedRecordUtil.getInvoiceLinesSegmentsValues(parsedRecord, "IMD+L+050+[5-4]");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenInvalidMappingExpressionIsSpecified() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);
    EdifactParsedRecordUtil.getInvoiceLinesSegmentsValues(parsedRecord, "IMD+L+050");
  }

}
