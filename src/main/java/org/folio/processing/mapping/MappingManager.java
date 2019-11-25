package org.folio.processing.mapping;

import org.folio.ProfileSnapshotWrapper;
import org.folio.processing.events.model.EventContext;
import org.folio.processing.mapping.mapper.FactoryRegistry;
import org.folio.processing.mapping.mapper.Mapper;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.model.MappingProfile;

import java.util.logging.Logger;

/**
 * MappingManager is the entry point to work with mapping.
 * Provides methods to register factories and start mapping.
 *
 * @see MappingProfile
 * @see FactoryRegistry
 * @see Reader
 * @see Writer
 */
public final class MappingManager {
  private final static Logger LOGGER = Logger.getLogger(MappingManager.class.getName());
  private static final FactoryRegistry FACTORY_REGISTRY = new FactoryRegistry();

  private MappingManager() {
  }

  /**
   * The entry point for mapping.
   *
   * @param eventContext event context
   * @return event context
   * @see MappingProfile
   * @see Mapper
   */
  public static EventContext map(EventContext eventContext) {
    try {
      ProfileSnapshotWrapper mappingProfileWrapper = eventContext.getCurrentNode();
      MappingProfile mappingProfile = (MappingProfile) mappingProfileWrapper.getContent();
      Reader reader = FACTORY_REGISTRY.createReader(mappingProfile.getIncomingRecordType());
      Writer writer = FACTORY_REGISTRY.createWriter(mappingProfile.getExistingRecordType());
      return new Mapper() {}.map(reader, writer, eventContext);
    } catch (Exception e) {
      LOGGER.warning(String.format("Exception occurred in Mapper [%s]", e));
      throw new RuntimeException(e);
    }
  }

  /**
   * Registers reader factory
   *
   * @param factory reader factory
   * @return true if registry changed as a result of the call
   */
  public static boolean registerReaderFactory(ReaderFactory factory) {
    return FACTORY_REGISTRY.registerReaderFactory(factory);
  }

  /**
   * Registers writer factory
   *
   * @param factory writer factory
   * @return true if registry changed as a result of the call
   */
  public static boolean registerWriterFactory(WriterFactory factory) {
    return FACTORY_REGISTRY.registerWriterFactory(factory);
  }
}
