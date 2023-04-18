package org.folio.processing.mapping.mapper.writer.marc;

import java.util.List;
import org.folio.Link;

public class LinkFull {
  private final Link link;
  private final String bibTag;
  private final List<String> bibSubfields;

  public LinkFull(Link link, String bibTag, List<String> bibSubfields) {
    this.link = link;
    this.bibTag = bibTag;
    this.bibSubfields = bibSubfields;
  }

  public Link getLink() {
    return link;
  }

  public String getBibTag() {
    return bibTag;
  }

  public List<String> getBibSubfields() {
    return bibSubfields;
  }
}
