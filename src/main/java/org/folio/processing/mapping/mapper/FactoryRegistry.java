package org.folio.processing.mapping.mapper;

import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static org.folio.processing.mapping.model.MappingProfile.EntityType;

/**
 * Registry for reader factories and writer factories
 */
public class FactoryRegistry {
    private static final List<ReaderFactory> READER_FACTORIES = new ArrayList<>();
    private static final List<WriterFactory> WRITER_FACTORIES = new ArrayList<>();

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
}
