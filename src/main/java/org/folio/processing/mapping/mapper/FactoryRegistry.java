package org.folio.processing.mapping.mapper;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.matcher.AbstractMatcher;
import org.folio.processing.matching.matcher.Matcher;
import org.folio.processing.matching.matcher.MatcherFactory;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.rest.jaxrs.model.EntityType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;

/**
 * Registry for reader factories and writer factories
 */
public class FactoryRegistry {
  private static final List<ReaderFactory> READER_FACTORIES = new ArrayList<>();
  private static final List<WriterFactory> WRITER_FACTORIES = new ArrayList<>();
  private static final List<MatcherFactory> MATCHER_FACTORIES = new ArrayList<>();

  /**
   * Creates reader by given entity type using reader factory
   *
   * @param entityType type of the entities which ReaderFactory produces
   * @return Reader
   */
  public Reader createReader(EntityType entityType) {
    Optional<ReaderFactory> optionalReaderFactory = READER_FACTORIES.stream()
      .filter(readerFactory -> readerFactory.isEligibleForEntityType(entityType))
      .findFirst();
    if (optionalReaderFactory.isPresent()) {
      ReaderFactory readerFactory = optionalReaderFactory.get();
      return readerFactory.createReader();
    } else {
      throw new IllegalArgumentException(format("Can not find ReaderFactory for entity type %s", entityType));
    }
  }

  /**
   * Creates writer by given entities type using writer factory
   *
   * @param entityType type of the entity which WriterFactory produces
   * @return Reader
   */
  public Writer createWriter(EntityType entityType) {
    Optional<WriterFactory> optionalWriterFactory = WRITER_FACTORIES.stream()
      .filter(writerFactory -> writerFactory.isEligibleForEntityType(entityType))
      .findFirst();
    if (optionalWriterFactory.isPresent()) {
      WriterFactory writerFactory = optionalWriterFactory.get();
      return writerFactory.createWriter();
    } else {
      throw new IllegalArgumentException(format("Can not find WriterFactory for entity type %s", entityType));
    }
  }

  /**
   * Creates matcher by given entities type using matcher factory
   *
   * @param entityType type of the entity which MatcherFactory produces
   * @return Reader
   */
  public Matcher createMatcher(EntityType entityType, MatchValueReader matchValueReader, MatchValueLoader matchValueLoader) {
    Optional<MatcherFactory> optionalWriterFactory = MATCHER_FACTORIES.stream()
      .filter(matcherFactory -> matcherFactory.isEligibleForEntityType(entityType))
      .findFirst();
    if (optionalWriterFactory.isPresent()) {
      MatcherFactory matcherFactory = optionalWriterFactory.get();
      return matcherFactory.createMatcher(matchValueReader, matchValueLoader);
    } else {
      return new AbstractMatcher(matchValueReader, matchValueLoader);
    }
  }

  /**
   * Returns list of registered reader factories
   *
   * @return list of reader factories
   */
  public List<ReaderFactory> getReaderFactories() {
    return READER_FACTORIES;
  }

  /**
   * Returns list of registered writer factories
   *
   * @return list of writer factories
   */
  public List<WriterFactory> getWriterFactories() {
    return WRITER_FACTORIES;
  }

  /**
   * Returns list of registered matcher factories
   *
   * @return list of matcher factories
   */
  public List<MatcherFactory> getMatcherFactories() {
    return MATCHER_FACTORIES;
  }
}
