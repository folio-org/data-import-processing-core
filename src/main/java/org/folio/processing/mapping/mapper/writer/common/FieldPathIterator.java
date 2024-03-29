package org.folio.processing.mapping.mapper.writer.common;

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *  Iterator for traversing by field path holding additional metadata for each path item
 */
class FieldPathIterator {
  private static final String DELIMITER_REGEX = "\\.";
  private Iterator<PathItem> delegate;

  FieldPathIterator(String path) {
    if (StringUtils.isEmpty(path)) {
      throw new IllegalArgumentException("Can not instantiate FieldPathIterator for empty path");
    } else {
      String[] stringItems = path.split(DELIMITER_REGEX);
      PathItem[] pathItems = Arrays.stream(stringItems).map(PathItem::new).toArray(PathItem[]::new);
      this.delegate = Arrays.stream(pathItems).iterator();
    }
  }

  /**
   * Returns {@code true} if the iteration has more elements.
   * (In other words, returns {@code true} if {@link #next} would
   * return an element rather than throwing an exception.)
   *
   * @return {@code true} if the iteration has more elements
   */
  boolean hasNext() {
    return this.delegate.hasNext();
  }

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   * @throws NoSuchElementException if the iteration has no more elements
   */
  PathItem next() {
    return this.delegate.next();
  }

  /**
   * Class to hold meta information for a single item of the fieldPath
   */
  class PathItem {
    private static final String ARRAY_SIGN = "[]";
    private String name;
    private boolean isArray;

    public PathItem(String path) {
      this.isArray = path.endsWith(ARRAY_SIGN);
      this.name = this.isArray ? path.replace(ARRAY_SIGN, StringUtils.EMPTY) : path;
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


