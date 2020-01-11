package org.folio.processing.mapping.mapper.writer.common;

import org.apache.commons.collections15.iterators.ObjectArrayIterator;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Iterator for traversing by field path holding additional metadata for each path item
 */
class FieldPathIterator {
  private static final String DELIMITER_REGEX = "\\.";
  private Iterator<PathItem> DELEGATE;

  FieldPathIterator(String path) {
    if (StringUtils.isEmpty(path)) {
      throw new IllegalArgumentException("Can not instantiate FieldPathIterator for empty path");
    } else {
      String[] stringItems = path.split(DELIMITER_REGEX);
      PathItem[] pathItems = Arrays.stream(stringItems).map(PathItem::new).toArray(PathItem[]::new);
      this.DELEGATE = new ObjectArrayIterator<>(pathItems);
    }
  }

  boolean hasNext() {
    return this.DELEGATE.hasNext();
  }

  PathItem next() {
    return this.DELEGATE.next();
  }

  class PathItem {
    private static final String ARRAY_SIGN = "[]";
    private String name;
    private boolean isArray;

    public PathItem(String item) {
      this.isArray = item.endsWith(ARRAY_SIGN);
      this.name = this.isArray ? item.replace(ARRAY_SIGN, StringUtils.EMPTY) : item;
    }

    public String getName() {
      return this.name;
    }

    boolean isArray() {
      return this.isArray;
    }

    boolean isObject() {
      return !this.isArray;
    }
  }
}


