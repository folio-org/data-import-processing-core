package org.folio.processing.mapping.mapper.writer.marc;

import java.util.List;
import org.folio.Link;

public record LinkFull(Link link, String bibTag, List<String> bibSubfields) {
}
