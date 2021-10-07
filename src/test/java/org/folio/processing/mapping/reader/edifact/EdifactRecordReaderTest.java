package org.folio.processing.mapping.reader.edifact;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.processing.mapping.mapper.reader.record.edifact.EdifactReaderFactory;
import org.folio.processing.mapping.mapper.reader.record.edifact.EdifactRecordReader;
import org.folio.processing.value.BooleanValue;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.RepeatableFieldValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.RepeatableSubfieldMapping;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;
import static org.folio.rest.jaxrs.model.MappingRule.BooleanFieldAction.ALL_TRUE;
import static org.folio.rest.jaxrs.model.MappingRule.RepeatableFieldAction.EXTEND_EXISTING;

public class EdifactRecordReaderTest {

  private static final String EDIFACT_PARSED_CONTENT = "{\"segments\": [{\"tag\": \"UNA\", \"dataElements\": []}, {\"tag\": \"UNB\", \"dataElements\": [{\"components\": [{\"data\": \"UNOC\"}, {\"data\": \"3\"}]}, {\"components\": [{\"data\": \"EBSCO\"}, {\"data\": \"92\"}]}, {\"components\": [{\"data\": \"KOH0002\"}, {\"data\": \"91\"}]}, {\"components\": [{\"data\": \"200610\"}, {\"data\": \"0105\"}]}, {\"components\": [{\"data\": \"5162\"}]}]}, {\"tag\": \"UNH\", \"dataElements\": [{\"components\": [{\"data\": \"5162\"}]}, {\"components\": [{\"data\": \"INVOIC\"}, {\"data\": \"D\"}, {\"data\": \"96A\"}, {\"data\": \"UN\"}, {\"data\": \"EAN008\"}]}]}, {\"tag\": \"BGM\", \"dataElements\": [{\"components\": [{\"data\": \"380\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"JINV\"}]}, {\"components\": [{\"data\": \"0704159\"}]}, {\"components\": [{\"data\": \"43\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"137\"}, {\"data\": \"20191002\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"NAD\", \"dataElements\": [{\"components\": [{\"data\": \"BY\"}]}, {\"components\": [{\"data\": \"BR1624506\"}, {\"data\": \"\"}, {\"data\": \"91\"}]}]}, {\"tag\": \"NAD\", \"dataElements\": [{\"components\": [{\"data\": \"SR\"}]}, {\"components\": [{\"data\": \"EBSCO\"}, {\"data\": \"\"}, {\"data\": \"92\"}]}]}, {\"tag\": \"CUX\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"004362033\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1941-6067\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1941-6067(20200101)14;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1941-6067(20201231)14;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"ACADEMY OF MANAGEMENT ANNALS -   ON\"}, {\"data\": \"LINE FOR INSTITUTIONS\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200101\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20201231\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"208.59\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"205\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S255699\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"SNA\"}, {\"data\": \"C6546362\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"LINE SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"3.59\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"006288237\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1944-737X\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1944-737X(20200301)117;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1944-737X(20210228)118;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"ACI MATERIALS JOURNAL - ONLINE   -\"}, {\"data\": \"MULTI USER\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200301\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20210228\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"726.5\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"714\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S283902\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"SNA\"}, {\"data\": \"E9498295\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"LINE SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"12.5\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"3\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"006289532\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1944-7361\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1944-7361(20200301)117;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1944-7361(20210228)118;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"GRADUATE PROGRAMS IN PHYSICS, ASTRO\"}, {\"data\": \"NOMY AND \"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"RELATED FIELDS.\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200301\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20210228\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"726.5\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"714\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S283901\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"SNA\"}, {\"data\": \"E9498296\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"LINE SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"12.5\"}]}]}, {\"tag\": \"UNS\", \"dataElements\": [{\"components\": [{\"data\": \"S\"}]}]}, {\"tag\": \"CNT\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}, {\"data\": \"3\"}]}]}, {\"tag\": \"CNT\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}, {\"data\": \"3\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"79\"}, {\"data\": \"18929.07\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"9\"}, {\"data\": \"18929.07\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"TOTAL SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"325.59\"}]}]}, {\"tag\": \"UNT\", \"dataElements\": [{\"components\": [{\"data\": \"294\"}]}, {\"components\": [{\"data\": \"5162-1\"}]}]}, {\"tag\": \"UNZ\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}]}, {\"components\": [{\"data\": \"5162\"}]}]}]}";
  private static final String INVOICE_LINE2_WITHOUT_ADJUSTMENTS_CONTENT = "{\"segments\": [{\"tag\": \"UNA\", \"dataElements\": []}, {\"tag\": \"UNB\", \"dataElements\": [{\"components\": [{\"data\": \"UNOC\"}, {\"data\": \"3\"}]}, {\"components\": [{\"data\": \"EBSCO\"}, {\"data\": \"92\"}]}, {\"components\": [{\"data\": \"KOH0002\"}, {\"data\": \"91\"}]}, {\"components\": [{\"data\": \"200610\"}, {\"data\": \"0105\"}]}, {\"components\": [{\"data\": \"5162\"}]}]}, {\"tag\": \"UNH\", \"dataElements\": [{\"components\": [{\"data\": \"5162-1\"}]}, {\"components\": [{\"data\": \"INVOIC\"}, {\"data\": \"D\"}, {\"data\": \"96A\"}, {\"data\": \"UN\"}, {\"data\": \"EAN008\"}]}]}, {\"tag\": \"BGM\", \"dataElements\": [{\"components\": [{\"data\": \"380\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"JINV\"}]}, {\"components\": [{\"data\": \"0704159\"}]}, {\"components\": [{\"data\": \"43\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"137\"}, {\"data\": \"20191002\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"NAD\", \"dataElements\": [{\"components\": [{\"data\": \"BY\"}]}, {\"components\": [{\"data\": \"BR1624506\"}, {\"data\": \"\"}, {\"data\": \"91\"}]}]}, {\"tag\": \"NAD\", \"dataElements\": [{\"components\": [{\"data\": \"SR\"}]}, {\"components\": [{\"data\": \"EBSCO\"}, {\"data\": \"\"}, {\"data\": \"92\"}]}]}, {\"tag\": \"CUX\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"004362033\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1941-6067\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1941-6067(20200101)14;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1941-6067(20201231)14;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"ACADEMY OF MANAGEMENT ANNALS -   ON\"}, {\"data\": \"LINE FOR INSTITUTIONS\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200101\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20201231\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"208.59\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"205\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S255699\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"SNA\"}, {\"data\": \"C6546362\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"LINE SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"3.59\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"006288237\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1944-737X\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1944-737X(20200301)117;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1944-737X(20210228)118;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"ACI MATERIALS JOURNAL - ONLINE   -\"}, {\"data\": \"MULTI USER\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200301\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20210228\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"726.5\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"714\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S283902\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"SNA\"}, {\"data\": \"E9498295\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"3\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"006289532\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1944-7361\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1944-7361(20200301)117;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1944-7361(20210228)118;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"ACI STRUCTURAL JOURNAL -   ON\"}, {\"data\": \"LINE - MULTI USER\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200301\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20210228\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"726.5\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"714\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S283901\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"SNA\"}, {\"data\": \"E9498296\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"LINE SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"12.5\"}]}]}, {\"tag\": \"UNS\", \"dataElements\": [{\"components\": [{\"data\": \"S\"}]}]}, {\"tag\": \"CNT\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}, {\"data\": \"3\"}]}]}, {\"tag\": \"CNT\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}, {\"data\": \"3\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"79\"}, {\"data\": \"18929.07\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"9\"}, {\"data\": \"18929.07\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"TOTAL SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"325.59\"}]}]}, {\"tag\": \"UNT\", \"dataElements\": [{\"components\": [{\"data\": \"294\"}]}, {\"components\": [{\"data\": \"5162-1\"}]}]}, {\"tag\": \"UNZ\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}]}, {\"components\": [{\"data\": \"5162\"}]}]}]}";
  private static final String INVOICE_LINE_IMD_WITHOUT_TARGET_DATA_ELEMENT_CONTENT = "{\"segments\": [{\"tag\": \"UNA\", \"dataElements\": []}, {\"tag\": \"UNB\", \"dataElements\": [{\"components\": [{\"data\": \"UNOC\"}, {\"data\": \"3\"}]}, {\"components\": [{\"data\": \"EBSCO\"}, {\"data\": \"92\"}]}, {\"components\": [{\"data\": \"KOH0002\"}, {\"data\": \"91\"}]}, {\"components\": [{\"data\": \"200610\"}, {\"data\": \"0105\"}]}, {\"components\": [{\"data\": \"5162\"}]}]}, {\"tag\": \"UNH\", \"dataElements\": [{\"components\": [{\"data\": \"5162\"}]}, {\"components\": [{\"data\": \"INVOIC\"}, {\"data\": \"D\"}, {\"data\": \"96A\"}, {\"data\": \"UN\"}, {\"data\": \"EAN008\"}]}]}, {\"tag\": \"BGM\", \"dataElements\": [{\"components\": [{\"data\": \"380\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"JINV\"}]}, {\"components\": [{\"data\": \"0704159\"}]}, {\"components\": [{\"data\": \"43\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"137\"}, {\"data\": \"20191002\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"NAD\", \"dataElements\": [{\"components\": [{\"data\": \"BY\"}]}, {\"components\": [{\"data\": \"BR1624506\"}, {\"data\": \"\"}, {\"data\": \"91\"}]}]}, {\"tag\": \"NAD\", \"dataElements\": [{\"components\": [{\"data\": \"SR\"}]}, {\"components\": [{\"data\": \"EBSCO\"}, {\"data\": \"\"}, {\"data\": \"92\"}]}]}, {\"tag\": \"CUX\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"004362033\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1941-6067\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1941-6067(20200101)14;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1941-6067(20201231)14;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"LAW IN CONTEXT SERIES\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200101\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20201231\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"208.59\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"205\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S255699\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"SNA\"}, {\"data\": \"C6546362\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"LINE SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"3.59\"}]}]}, {\"tag\": \"UNS\", \"dataElements\": [{\"components\": [{\"data\": \"S\"}]}]}, {\"tag\": \"CNT\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}, {\"data\": \"3\"}]}]}, {\"tag\": \"CNT\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}, {\"data\": \"3\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"79\"}, {\"data\": \"18929.07\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"9\"}, {\"data\": \"18929.07\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"TOTAL SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"325.59\"}]}]}, {\"tag\": \"UNT\", \"dataElements\": [{\"components\": [{\"data\": \"294\"}]}, {\"components\": [{\"data\": \"5162-1\"}]}]}, {\"tag\": \"UNZ\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}]}, {\"components\": [{\"data\": \"5162\"}]}]}]}";

  private final ReaderFactory readerFactory = new EdifactReaderFactory();
  private final MappingContext mappingContext = new MappingContext();

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenPayloadHasNoRecord() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    dataImportEventPayload.setContext(new HashMap<>());

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenPayloadHasNoParsedRecordContentRecord() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord())));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);
  }

  @Test
  public void shouldReadStringConstantFromMappingRule() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    Value value = reader.read(new MappingRule().withPath("invoice.status").withValue("\"Open\""));

    Assert.assertEquals(Value.ValueType.STRING, value.getType());
    Assert.assertEquals("Open", value.getValue());
  }

  @Test
  public void shouldReturnStringValue() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    Value value = reader.read(new MappingRule().withPath("invoice.lockTotal").withValue("MOA+9[2]"));

    Assert.assertEquals(Value.ValueType.STRING, value.getType());
    Assert.assertEquals("18929.07", value.getValue());
  }

  @Test
  public void shouldReadMappingRuleWithDataPositionsRange() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    Value value = reader.read(new MappingRule().withPath("invoice.note").withValue("UNH+5162+[1-3]"));

    Assert.assertEquals(Value.ValueType.STRING, value.getType());
    Assert.assertEquals("INVOICD96A", value.getValue());
  }

  @Test
  public void shouldReturnMissingValueWhenMappingRuleHasNoMappingExpression() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    Value value = reader.read(new MappingRule().withPath("invoice.note").withValue(""));

    Assert.assertEquals(Value.ValueType.MISSING, value.getType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenMappingRuleHasInvalidPositionsRange() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    reader.read(new MappingRule().withPath("invoice.note").withValue("UNH+5162+[2-1]"));
  }

  @Test
  public void shouldFormatDateToIsoFormatWhenDateTimeSegmentIsSpecifiedInMappingRule() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    Value value = reader.read(new MappingRule().withPath("invoice.invoiceDate").withValue("DTM+137[2]"));

    Assert.assertEquals(Value.ValueType.STRING, value.getType());
    Assert.assertEquals("2019-10-02T00:00:00.000+0000", value.getValue());
  }

  @Test
  public void shouldReturnStringValueWhenMappingExpressionHasQualifier() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    // when
    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);
    Value value = reader.read(new MappingRule().withPath("invoice.lockTotal").withValue("CUX+2?4[2]"));

    // then
    Assert.assertEquals(Value.ValueType.STRING, value.getType());
    Assert.assertEquals("USD", value.getValue());
  }

  @Test
  public void shouldReadBooleanValueWhenMappingRuleHasBooleanFieldAction() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    // when
    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);
    Value value = reader.read(new MappingRule().withPath("invoice.chkSubscriptionOverlap").withBooleanFieldAction(ALL_TRUE));

    // then
    Assert.assertEquals(Value.ValueType.BOOLEAN, value.getType());
    Assert.assertEquals(ALL_TRUE, value.getValue());
  }

  @Test
  public void shouldReadAndReturnMissingValueIfMappingExpressionIsEmpty() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    // when
    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);
    Value value = reader.read(new MappingRule().withPath("invoice.chkSubscriptionOverlap"));

    // then
    Assert.assertEquals(Value.ValueType.MISSING, value.getType());
  }

  @Test
  public void shouldReturnListValueWhenMappingRuleHasArrayFieldPath() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    HashMap<String, String> acqUnitsAcceptedValues = new HashMap<>(Map.of(
      "b2c0e100-0485-43f2-b161-3c60aac9f68a", "ackUnit-1",
      "b2c0e100-0485-43f2-b161-3c60aac9f128", "ackUnit-2",
      "b2c0e100-0485-43f2-b161-3c60aac9f256", "ackUnit-3"));

    MappingRule mappingRule = new MappingRule().withPath("invoice.acqUnitIds[]")
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withAcceptedValues(acqUnitsAcceptedValues)
      .withSubfields(Arrays.asList(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("invoice.acqUnitIds[]")
          .withFields(singletonList(
            new MappingRule()
              .withPath("invoice.acqUnitIds[]")
              .withValue("\"ackUnit-1\""))),
        new RepeatableSubfieldMapping()
          .withOrder(1)
          .withPath("invoice.acqUnitIds[]")
          .withFields(singletonList(
            new MappingRule()
              .withPath("invoice.acqUnitIds[]")
              .withValue("\"ackUnit-2\"")))
      ));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    // when
    Value value = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.LIST, value.getType());
    Assert.assertEquals(Arrays.asList("b2c0e100-0485-43f2-b161-3c60aac9f68a", "b2c0e100-0485-43f2-b161-3c60aac9f128"), value.getValue());
  }

  @Test
  public void shouldReturnMissingValueWhenMappingRuleHasArrayFieldPathAndSubfieldRulesHaveNoValue() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    MappingRule mappingRule = new MappingRule().withPath("invoice.acqUnitIds[]")
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(Arrays.asList(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath("invoice.acqUnitIds[]")
          .withFields(singletonList(
            new MappingRule()
              .withPath("invoice.acqUnitIds[]")
              .withValue(""))),
        new RepeatableSubfieldMapping()
          .withOrder(1)
          .withPath("invoice.acqUnitIds[]")
          .withFields(singletonList(
            new MappingRule()
              .withPath("invoice.acqUnitIds[]")
              .withValue("")))));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    // when
    Value value = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.MISSING, value.getType());
  }

  @Test
  public void shouldReturnRepeatableFieldValue() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    HashMap<String, String> fundAcceptedValues = new HashMap<>(Map.of(
      "b2c0e100-0485-43f2-b161-3c60aac9f687", "fund-1",
      "b2c0e100-0485-43f2-b161-3c60aac9f177", "fund-2",
      "b2c0e100-0485-43f2-b161-3c60aac9f777", "fund-3"));

    String rootPath = "invoice.adjustments[]";
    String fundDistributionsRootPath = "invoice.adjustments[].fundDistributions[]";
    MappingRule mappingRule = new MappingRule().withPath(rootPath)
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(singletonList(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath(rootPath)
          .withFields(Arrays.asList(
            new MappingRule()
              .withPath("invoice.adjustments[].description")
              .withValue("\"description-1\""),
            new MappingRule()
              .withPath("invoice.adjustments[].exportToAccounting")
              .withBooleanFieldAction(ALL_TRUE),
            new MappingRule()
              .withPath("invoice.adjustments[].fundDistributions[]")
              .withRepeatableFieldAction(EXTEND_EXISTING)
              .withSubfields(singletonList(new RepeatableSubfieldMapping()
                .withOrder(0)
                .withPath(fundDistributionsRootPath)
                .withFields(Arrays.asList(
                  new MappingRule()
                    .withPath("invoice.adjustments[].fundDistributions[].fundId")
                    .withValue("\"fund-3\"")
                    .withAcceptedValues(fundAcceptedValues),
                  new MappingRule()
                    .withPath("invoice.adjustments[].fundDistributions[].code")
                    .withValue("\"USHIST\""))))))
          )));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    // when
    Value actualValue = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.REPEATABLE, actualValue.getType());
    RepeatableFieldValue repeatableFieldValue = (RepeatableFieldValue) actualValue;
    Assert.assertEquals(rootPath, repeatableFieldValue.getRootPath());
    Assert.assertEquals(EXTEND_EXISTING, repeatableFieldValue.getRepeatableFieldAction());

    Map<String, Value> expectedFundDistributionElement = Map.of(
      "invoice.adjustments[].fundDistributions[].fundId", StringValue.of("b2c0e100-0485-43f2-b161-3c60aac9f777"),
      "invoice.adjustments[].fundDistributions[].code", StringValue.of("USHIST"));

    Map<String, Value> expectedAdjustments = Map.of(
      "invoice.adjustments[].description", StringValue.of("description-1"),
      "invoice.adjustments[].exportToAccounting", BooleanValue.of(ALL_TRUE),
      "invoice.adjustments[].fundDistributions[]", RepeatableFieldValue.of(List.of(expectedFundDistributionElement), EXTEND_EXISTING, fundDistributionsRootPath));

    RepeatableFieldValue expectedValue = RepeatableFieldValue.of(List.of(expectedAdjustments), EXTEND_EXISTING, rootPath);
    Assert.assertEquals(JsonObject.mapFrom(expectedValue), JsonObject.mapFrom(actualValue));
  }

  @Test
  public void shouldReadMappingRuleWithElseClause() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    String expressionWithElseClause = "MOA+86[2]; else MOA+9[2]";
    String rootPath = "invoice.adjustments[]";
    String fundDistributionsRootPath = "invoice.adjustments[].fundDistributions[]";
    MappingRule mappingRule = new MappingRule().withPath(rootPath)
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(singletonList(
        new RepeatableSubfieldMapping()
          .withOrder(0)
          .withPath(rootPath)
          .withFields(Arrays.asList(
            new MappingRule()
              .withPath("invoice.adjustments[].description")
              .withValue("\"test adjustment\""),
            new MappingRule()
              .withPath("invoice.adjustments[].fundDistributions[]")
              .withRepeatableFieldAction(EXTEND_EXISTING)
              .withSubfields(singletonList(new RepeatableSubfieldMapping()
                .withOrder(0)
                .withPath(fundDistributionsRootPath)
                .withFields(Arrays.asList(
                  new MappingRule()
                    .withPath("invoice.adjustments[].fundDistributions[].value")
                    .withValue(expressionWithElseClause),
                  new MappingRule()
                    .withPath("invoice.adjustments[].fundDistributions[].code")
                    .withValue("\"USHIST\""))))))
          )));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    // when
    Value actualValue = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.REPEATABLE, actualValue.getType());
    RepeatableFieldValue repeatableFieldValue = (RepeatableFieldValue) actualValue;
    Assert.assertEquals(rootPath, repeatableFieldValue.getRootPath());
    Assert.assertEquals(EXTEND_EXISTING, repeatableFieldValue.getRepeatableFieldAction());

    Map<String, Value> expectedFundDistributionElement = Map.of(
      "invoice.adjustments[].fundDistributions[].value", StringValue.of("18929.07"),
      "invoice.adjustments[].fundDistributions[].code", StringValue.of("USHIST"));

    Map<String, Value> expectedAdjustments = Map.of(
      "invoice.adjustments[].description", StringValue.of("test adjustment"),
      "invoice.adjustments[].fundDistributions[]", RepeatableFieldValue.of(List.of(expectedFundDistributionElement), EXTEND_EXISTING, fundDistributionsRootPath));

    RepeatableFieldValue expectedValue = RepeatableFieldValue.of(List.of(expectedAdjustments), EXTEND_EXISTING, rootPath);
    Assert.assertEquals(JsonObject.mapFrom(expectedValue), JsonObject.mapFrom(actualValue));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenMappingRuleHasInvalidMappingSyntax() throws IOException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);
    reader.read(new MappingRule().withPath("invoice.status").withValue("bla expression"));
  }

  @Test
  public void shouldReturnRepeatableFieldValueForInvoiceLineMappingRule() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    dataImportEventPayload.setContext(context);

    HashMap<String, String> fundIdAcceptedValues = new HashMap<>(Map.of(
      "6506b79b-7702-48b2-9774-a1c538fdd34e", "Gifts (GIFTS-ONE-TIME)",
      "1b6d3338-186e-4e35-9e75-1b886b0da53e", "Grants (GRANT-SUBN)",
      "65032151-39a5-4cef-8810-5350eb316300", "US History (USHIST)"));

    String rootPath = "invoice.invoiceLines[]";
    String adjustmentsPath = "invoice.invoiceLines[].adjustments[]";
    String fundDistributionsPath = "invoice.invoiceLines[].fundDistributions[]";
    String referenceNumbersPath = "invoice.invoiceLines[].referenceNumbers[]";

    MappingRule mappingRule = new MappingRule().withPath(rootPath)
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(List.of(new RepeatableSubfieldMapping()
        .withOrder(0)
        .withPath(rootPath)
        .withFields(List.of(
          new MappingRule()
            .withPath("invoice.invoiceLines[].invoiceLineStatus")
            .withValue("\"Open\""),
          new MappingRule()
            .withPath("invoice.invoiceLines[].description")
            .withValue("IMD+L+050+[4-5]"),
          new MappingRule()
            .withPath(referenceNumbersPath)
            .withRepeatableFieldAction(EXTEND_EXISTING)
            .withSubfields(List.of(new RepeatableSubfieldMapping()
              .withOrder(0)
              .withPath(referenceNumbersPath)
              .withFields(List.of(
                new MappingRule().withPath("invoice.invoiceLines[].referenceNumbers[].refNumber")
                  .withValue("RFF+SNA[2]"),
                new MappingRule().withPath("invoice.invoiceLines[].referenceNumbers[].refNumberType")
                  .withValue("\"Vendor continuation reference number\""))))),
          new MappingRule()
            .withPath(adjustmentsPath)
            .withRepeatableFieldAction(EXTEND_EXISTING)
            .withSubfields(List.of(new RepeatableSubfieldMapping()
              .withOrder(0)
              .withPath(adjustmentsPath)
              .withFields(List.of(
                new MappingRule().withPath("invoice.invoiceLines[].adjustments[].description")
                  .withValue("ALC+C++++[4]"),
                new MappingRule().withPath("invoice.invoiceLines[].adjustments[].value")
                  .withValue("MOA+8[2]"),
                new MappingRule().withPath("invoice.invoiceLines[].adjustments[].exportToAccounting")
                  .withBooleanFieldAction(ALL_TRUE))))),
          new MappingRule()
            .withPath(fundDistributionsPath)
            .withRepeatableFieldAction(EXTEND_EXISTING)
            .withSubfields(List.of(new RepeatableSubfieldMapping()
              .withOrder(0)
              .withPath(fundDistributionsPath)
              .withFields(List.of(
                new MappingRule().withPath("invoice.invoiceLines[].fundDistributions[].fundId")
                  .withValue("\"US History (USHIST)\"")
                  .withAcceptedValues(fundIdAcceptedValues),
                new MappingRule().withPath("invoice.invoiceLines[].fundDistributions[].distributionType")
                  .withValue("\"percentage\"")))))
        ))));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    // when
    Value value = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.REPEATABLE, value.getType());
    RepeatableFieldValue actualValue = (RepeatableFieldValue) value;
    Assert.assertEquals(rootPath, actualValue.getRootPath());
    Assert.assertEquals(EXTEND_EXISTING, actualValue.getRepeatableFieldAction());

    Map<String, Value> expectedAdjustment1 = Map.of(
      "invoice.invoiceLines[].adjustments[].description", StringValue.of("LINE SERVICE CHARGE"),
      "invoice.invoiceLines[].adjustments[].value", StringValue.of("3.59"),
      "invoice.invoiceLines[].adjustments[].exportToAccounting", BooleanValue.of(ALL_TRUE));
    Map<String, Value> expectedAdjustment2 = Map.of(
      "invoice.invoiceLines[].adjustments[].description", StringValue.of("LINE SERVICE CHARGE"),
      "invoice.invoiceLines[].adjustments[].value", StringValue.of("12.5"),
      "invoice.invoiceLines[].adjustments[].exportToAccounting", BooleanValue.of(ALL_TRUE));
    Map<String, Value> expectedAdjustment3 = Map.of(
      "invoice.invoiceLines[].adjustments[].description", StringValue.of("LINE SERVICE CHARGE"),
      "invoice.invoiceLines[].adjustments[].value", StringValue.of("12.5"),
      "invoice.invoiceLines[].adjustments[].exportToAccounting", BooleanValue.of(ALL_TRUE));

    Map<String, Value> fundDistribution = Map.of(
      "invoice.invoiceLines[].fundDistributions[].fundId", StringValue.of("65032151-39a5-4cef-8810-5350eb316300"),
      "invoice.invoiceLines[].fundDistributions[].distributionType", StringValue.of("percentage"));

    Map<String, Value> expectedReferenceNumber1 = Map.of(
      "invoice.invoiceLines[].referenceNumbers[].refNumber", StringValue.of("C6546362"),
      "invoice.invoiceLines[].referenceNumbers[].refNumberType", StringValue.of("Vendor continuation reference number"));
    Map<String, Value> expectedReferenceNumber2 = Map.of(
      "invoice.invoiceLines[].referenceNumbers[].refNumber", StringValue.of("E9498295"),
      "invoice.invoiceLines[].referenceNumbers[].refNumberType", StringValue.of("Vendor continuation reference number"));
    Map<String, Value> expectedReferenceNumber3 = Map.of(
      "invoice.invoiceLines[].referenceNumbers[].refNumber", StringValue.of("E9498296"),
      "invoice.invoiceLines[].referenceNumbers[].refNumberType", StringValue.of("Vendor continuation reference number"));

    List<Map<String, Value>> expectedInvoiceLines = List.of(
      Map.of("invoice.invoiceLines[].description", StringValue.of("ACADEMY OF MANAGEMENT ANNALS -   ONLINE FOR INSTITUTIONS"),
        "invoice.invoiceLines[].invoiceLineStatus", StringValue.of("Open"),
        adjustmentsPath, RepeatableFieldValue.of(List.of(expectedAdjustment1), EXTEND_EXISTING, adjustmentsPath),
        fundDistributionsPath, RepeatableFieldValue.of(List.of(fundDistribution), EXTEND_EXISTING, fundDistributionsPath),
        referenceNumbersPath, RepeatableFieldValue.of(List.of(expectedReferenceNumber1), EXTEND_EXISTING, referenceNumbersPath)),
      Map.of("invoice.invoiceLines[].description", StringValue.of("ACI MATERIALS JOURNAL - ONLINE   -MULTI USER"),
        "invoice.invoiceLines[].invoiceLineStatus", StringValue.of("Open"),
        adjustmentsPath, RepeatableFieldValue.of(List.of(expectedAdjustment2), EXTEND_EXISTING, adjustmentsPath),
        fundDistributionsPath, RepeatableFieldValue.of(List.of(fundDistribution), EXTEND_EXISTING, fundDistributionsPath),
        referenceNumbersPath, RepeatableFieldValue.of(List.of(expectedReferenceNumber2), EXTEND_EXISTING, referenceNumbersPath)),
      Map.of("invoice.invoiceLines[].description", StringValue.of("GRADUATE PROGRAMS IN PHYSICS, ASTRONOMY AND RELATED FIELDS."),
        "invoice.invoiceLines[].invoiceLineStatus", StringValue.of("Open"),
        adjustmentsPath, RepeatableFieldValue.of(List.of(expectedAdjustment3), EXTEND_EXISTING, adjustmentsPath),
        fundDistributionsPath, RepeatableFieldValue.of(List.of(fundDistribution), EXTEND_EXISTING, fundDistributionsPath),
        referenceNumbersPath, RepeatableFieldValue.of(List.of(expectedReferenceNumber3), EXTEND_EXISTING, referenceNumbersPath)));

    RepeatableFieldValue expectedValue = RepeatableFieldValue.of(expectedInvoiceLines, EXTEND_EXISTING, rootPath);
    Assert.assertEquals(JsonObject.mapFrom(expectedValue), JsonObject.mapFrom(actualValue));
  }

  @Test
  public void shouldSetMissingValueToInvoiceLineAdjustmentsWhenRecordHasNoAdjustmentsData() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(INVOICE_LINE2_WITHOUT_ADJUSTMENTS_CONTENT))));
    dataImportEventPayload.setContext(context);

    String rootPath = "invoice.invoiceLines[]";
    String adjustmentsPath = "invoice.invoiceLines[].adjustments[]";

    MappingRule mappingRule = new MappingRule().withPath(rootPath)
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(List.of(new RepeatableSubfieldMapping()
        .withOrder(0)
        .withPath(rootPath)
        .withFields(List.of(
          new MappingRule()
            .withPath("invoice.invoiceLines[].invoiceLineStatus")
            .withValue("\"Open\""),
          new MappingRule()
            .withPath(adjustmentsPath)
            .withRepeatableFieldAction(EXTEND_EXISTING)
            .withSubfields(List.of(new RepeatableSubfieldMapping()
              .withOrder(0)
              .withPath(adjustmentsPath)
              .withFields(List.of(
                new MappingRule().withPath("invoice.invoiceLines[].adjustments[].value")
                  .withValue("MOA+8[2]")))))
        ))));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    // when
    Value value = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.REPEATABLE, value.getType());
    RepeatableFieldValue actualValue = (RepeatableFieldValue) value;
    Assert.assertEquals(rootPath, actualValue.getRootPath());
    Assert.assertEquals(EXTEND_EXISTING, actualValue.getRepeatableFieldAction());

    Map<String, Value> expectedAdjustment1 = Map.of("invoice.invoiceLines[].adjustments[].value", StringValue.of("3.59"));
    Map<String, Value> expectedAdjustment3 = Map.of("invoice.invoiceLines[].adjustments[].value", StringValue.of("12.5"));

    List<Map<String, Value>> expectedInvoiceLines = List.of(
      Map.of("invoice.invoiceLines[].invoiceLineStatus", StringValue.of("Open"),
        adjustmentsPath, RepeatableFieldValue.of(List.of(expectedAdjustment1), EXTEND_EXISTING, adjustmentsPath)),
      Map.of("invoice.invoiceLines[].invoiceLineStatus", StringValue.of("Open"),
        adjustmentsPath, MissingValue.getInstance()),
      Map.of("invoice.invoiceLines[].invoiceLineStatus", StringValue.of("Open"),
        adjustmentsPath, RepeatableFieldValue.of(List.of(expectedAdjustment3), EXTEND_EXISTING, adjustmentsPath)));

    RepeatableFieldValue expectedValue = RepeatableFieldValue.of(expectedInvoiceLines, EXTEND_EXISTING, rootPath);
    Assert.assertEquals(JsonObject.mapFrom(expectedValue), JsonObject.mapFrom(actualValue));
  }

  @Test
  public void shouldReadInvoiceLineDescriptionFromPOLineExternalData() throws IOException {
    // given
    String expectedPOLineTitle1 = "POLineTitle-1";
    String expectedPOLineTitle3 = "POLineTitle-3";
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));
    context.put("POL_TITLE_0", expectedPOLineTitle1);
    context.put("POL_TITLE_2", expectedPOLineTitle3);
    dataImportEventPayload.setContext(context);

    String rootPath = "invoice.invoiceLines[]";

    MappingRule mappingRule = new MappingRule().withPath(rootPath)
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(List.of(new RepeatableSubfieldMapping()
        .withOrder(0)
        .withPath(rootPath)
        .withFields(List.of(
          new MappingRule()
            .withPath("invoice.invoiceLines[].invoiceLineStatus")
            .withValue("\"Open\""),
          new MappingRule()
            .withPath("invoice.invoiceLines[].description")
            .withValue("{POL_title}; else IMD+L+050+[4]")))));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    // when
    Value value = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.REPEATABLE, value.getType());
    RepeatableFieldValue actualValue = (RepeatableFieldValue) value;
    Assert.assertEquals(rootPath, actualValue.getRootPath());
    Assert.assertEquals(EXTEND_EXISTING, actualValue.getRepeatableFieldAction());

    List<Map<String, Value>> expectedInvoiceLines = List.of(
      Map.of("invoice.invoiceLines[].description", StringValue.of(expectedPOLineTitle1),
        "invoice.invoiceLines[].invoiceLineStatus", StringValue.of("Open")),
      Map.of("invoice.invoiceLines[].description", StringValue.of("ACI MATERIALS JOURNAL - ONLINE   -"),
        "invoice.invoiceLines[].invoiceLineStatus", StringValue.of("Open")),
      Map.of("invoice.invoiceLines[].description", StringValue.of(expectedPOLineTitle3),
        "invoice.invoiceLines[].invoiceLineStatus", StringValue.of("Open")));

    RepeatableFieldValue expectedValue = RepeatableFieldValue.of(expectedInvoiceLines, EXTEND_EXISTING, rootPath);
    Assert.assertEquals(JsonObject.mapFrom(expectedValue), JsonObject.mapFrom(actualValue));
  }

  @Test
  public void shouldReadInvoiceLineFundDistributionFromPOLineExternalData() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT))));

    JsonArray fundDistributions1 = new JsonArray()
      .add(new JsonObject().put("code", "USHIST").put("fundId", "1d1574f1-9196-4a57-8d1f-3b2e4309eb81"));
    JsonArray fundDistributions3 = new JsonArray()
      .add(new JsonObject().put("code", "EUHIST").put("fundId", "63157e96-0693-426d-b0df-948bacdfdb08"));

    context.put("POL_FUND_DISTRIBUTIONS_0", fundDistributions1.encode());
    context.put("POL_FUND_DISTRIBUTIONS_2", fundDistributions3.encode());
    dataImportEventPayload.setContext(context);

    String rootPath = "invoice.invoiceLines[]";
    String fundDistributionsPath = "invoice.invoiceLines[].fundDistributions[]";

    MappingRule mappingRule = new MappingRule().withPath(rootPath)
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(singletonList(new RepeatableSubfieldMapping()
        .withOrder(0)
        .withPath(rootPath)
        .withFields(Arrays.asList(
          new MappingRule()
            .withPath("invoice.invoiceLines[].invoiceLineStatus")
            .withValue("\"Open\""),
          new MappingRule()
            .withPath(fundDistributionsPath)
            .withRepeatableFieldAction(EXTEND_EXISTING)
            .withValue("{POL_FUND_DISTRIBUTIONS}")
        ))));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    // when
    Value value = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.REPEATABLE, value.getType());
    RepeatableFieldValue actualValue = (RepeatableFieldValue) value;
    Assert.assertEquals(rootPath, actualValue.getRootPath());
    Assert.assertEquals(EXTEND_EXISTING, actualValue.getRepeatableFieldAction());

    Map<String, Value> expectedFundDistributions1 = Map.of(
      "invoice.invoiceLines[].fundDistributions[].fundId", StringValue.of("1d1574f1-9196-4a57-8d1f-3b2e4309eb81"),
      "invoice.invoiceLines[].fundDistributions[].code", StringValue.of("USHIST"));
    Map<String, Value> expectedFundDistributions3 = Map.of(
      "invoice.invoiceLines[].fundDistributions[].fundId", StringValue.of("63157e96-0693-426d-b0df-948bacdfdb08"),
      "invoice.invoiceLines[].fundDistributions[].code", StringValue.of("EUHIST"));

    List<Map<String, Value>> expectedInvoiceLines = List.of(
      Map.of("invoice.invoiceLines[].invoiceLineStatus", StringValue.of("Open"),
        fundDistributionsPath, RepeatableFieldValue.of(List.of(expectedFundDistributions1), EXTEND_EXISTING, fundDistributionsPath)),
      Map.of("invoice.invoiceLines[].invoiceLineStatus", StringValue.of("Open"),
        fundDistributionsPath, MissingValue.getInstance()),
      Map.of("invoice.invoiceLines[].invoiceLineStatus", StringValue.of("Open"),
        fundDistributionsPath, RepeatableFieldValue.of(List.of(expectedFundDistributions3), EXTEND_EXISTING, fundDistributionsPath)));

    RepeatableFieldValue expectedValue = RepeatableFieldValue.of(expectedInvoiceLines, EXTEND_EXISTING, rootPath);
    Assert.assertEquals(JsonObject.mapFrom(expectedValue), JsonObject.mapFrom(actualValue));
  }

  @Test
  public void shouldReturnValueWhenTargetSegmentDataElementDoesNotExist() throws IOException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    HashMap<String, String> context = new HashMap<>();
    context.put(EDIFACT_INVOICE.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(INVOICE_LINE_IMD_WITHOUT_TARGET_DATA_ELEMENT_CONTENT))));
    dataImportEventPayload.setContext(context);

    String rootPath = "invoice.invoiceLines[]";
    MappingRule mappingRule = new MappingRule().withPath(rootPath)
      .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
      .withSubfields(List.of(new RepeatableSubfieldMapping()
        .withOrder(0)
        .withPath(rootPath)
        .withFields(List.of(new MappingRule()
          .withPath("invoice.invoiceLines[].description")
          .withValue("IMD+L+050+[4-5]")
        ))));

    Reader reader = readerFactory.createReader();
    reader.initialize(dataImportEventPayload, mappingContext);

    // when
    Value value = reader.read(mappingRule);

    // then
    Assert.assertEquals(Value.ValueType.REPEATABLE, value.getType());
    RepeatableFieldValue actualValue = (RepeatableFieldValue) value;

    List<Map<String, Value>> expectedInvoiceLines = List.of(
      Map.of("invoice.invoiceLines[].description", StringValue.of("LAW IN CONTEXT SERIES")));

    RepeatableFieldValue expectedValue = RepeatableFieldValue.of(expectedInvoiceLines, EXTEND_EXISTING, rootPath);
    Assert.assertEquals(JsonObject.mapFrom(expectedValue), JsonObject.mapFrom(actualValue));
  }

  @Test
  public void shouldReturnCorrespondingValueForAllInvoiceLines() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);

    Map<Integer, String> actualSegmentsValues = EdifactRecordReader.getInvoiceLinesSegmentsValues(parsedRecord, "RFF+LI[2]");

    Assert.assertEquals(3, actualSegmentsValues.size());
    Assert.assertEquals("S255699", actualSegmentsValues.get(1));
    Assert.assertEquals("S283902", actualSegmentsValues.get(2));
    Assert.assertEquals("S283901", actualSegmentsValues.get(3));
  }

  @Test
  public void shouldReturnValuesForExistingInvoiceLinesSegments() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(INVOICE_LINE2_WITHOUT_ADJUSTMENTS_CONTENT);

    Map<Integer, String> actualSegmentsValues = EdifactRecordReader.getInvoiceLinesSegmentsValues(parsedRecord, "MOA+8[2]");

    Assert.assertEquals(2, actualSegmentsValues.size());
    Assert.assertEquals("3.59", actualSegmentsValues.get(1));
    Assert.assertEquals("12.5", actualSegmentsValues.get(3));
  }

  @Test
  public void shouldReturnValuesWhenMappingExpressionHasQualifier() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);

    Map<Integer, String> actualSegmentsValues = EdifactRecordReader.getInvoiceLinesSegmentsValues(parsedRecord, "MOA+203?4[2]");

    Assert.assertEquals(3, actualSegmentsValues.size());
    Assert.assertEquals("208.59", actualSegmentsValues.get(1));
    Assert.assertEquals("726.5", actualSegmentsValues.get(2));
    Assert.assertEquals("726.5", actualSegmentsValues.get(3));
  }

  @Test
  public void shouldReturnValuesByDataPositionsRange() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);

    Map<Integer, String> actualSegmentsValues = EdifactRecordReader.getInvoiceLinesSegmentsValues(parsedRecord, "IMD+L+050+[4-5]");

    Assert.assertEquals(3, actualSegmentsValues.size());
    Assert.assertEquals("ACADEMY OF MANAGEMENT ANNALS -   ONLINE FOR INSTITUTIONS", actualSegmentsValues.get(1));
    Assert.assertEquals("ACI MATERIALS JOURNAL - ONLINE   -MULTI USER", actualSegmentsValues.get(2));
    Assert.assertEquals("GRADUATE PROGRAMS IN PHYSICS, ASTRONOMY AND RELATED FIELDS.", actualSegmentsValues.get(3));
  }

  @Test
  public void shouldReturnEmptyMapWhenInvoiceLinesHaveNoSpecifiedSegment() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);

    Map<Integer, String> actualSegmentsValues = EdifactRecordReader.getInvoiceLinesSegmentsValues(parsedRecord, "IMD+F+050+[4]");

    Assert.assertTrue(actualSegmentsValues.isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenParsedRecordHasNoParsedContent() {
    ParsedRecord parsedRecord = new ParsedRecord();
    EdifactRecordReader.getInvoiceLinesSegmentsValues(parsedRecord, "RFF+SNA[2]");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenMappingExpressionHasInvalidPositionsRange() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);
    EdifactRecordReader.getInvoiceLinesSegmentsValues(parsedRecord, "IMD+L+050+[5-4]");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenInvalidMappingExpressionIsSpecified() {
    ParsedRecord parsedRecord = new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT);
    EdifactRecordReader.getInvoiceLinesSegmentsValues(parsedRecord, "IMD+L+050");
  }

}
